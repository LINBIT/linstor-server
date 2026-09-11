package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandlerUtils;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeSatelliteFactory;
import com.linbit.linstor.logging.BaseErrorReporter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerOffline;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class ControllerPeerConnectorImpl implements ControllerPeerConnector
{
    private final CoreModule.NodesMap nodesMap;

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    private final ErrorReporter errorReporter;

    private final NodeSatelliteFactory nodeFactory;

    private final Provider<TransactionMgr> transMgrProvider;
    private final CommonSerializer commonSerializer;

    // Local NodeName received from the currently active controller
    private @Nullable NodeName localNodeName;

    // The currently connected controller peer
    private final PeerOffline offlineCtrlPeer;
    private @Nullable UUID ctrlUuid;
    private Peer controllerPeer;
    private final Provider<StltApiCallHandlerUtils> stltApiCallHandlerUtils;

    private final StltExtToolsChecker stltExtToolsChecker;

    @Inject
    public ControllerPeerConnectorImpl(
        CoreModule.NodesMap nodesMapRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        ErrorReporter errorReporterRef,
        NodeSatelliteFactory nodeFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CommonSerializer commonSerializerRef,
        StltConnTracker stltConnTracker,
        Provider<StltApiCallHandlerUtils> stltApiCallHandlerUtilsProviderRef,
        StltExtToolsChecker stltExtToolsCheckerRef
    )
    {
        nodesMap = nodesMapRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        errorReporter = errorReporterRef;
        nodeFactory = nodeFactoryRef;
        transMgrProvider = transMgrProviderRef;
        commonSerializer = commonSerializerRef;
        stltApiCallHandlerUtils = stltApiCallHandlerUtilsProviderRef;
        stltExtToolsChecker = stltExtToolsCheckerRef;

        offlineCtrlPeer = new PeerOffline(errorReporterRef, "controller", null);
        controllerPeer = offlineCtrlPeer;

        // if we lose the connection to the current controller, we should set our ctrlPeer to offlineCtrlPeer
        // otherwise, once the controller reconnects, we send our "old" controller an "OtherController"
        // message.
        stltConnTracker.addClosingListener((peer, isCtrl) ->
        {
            if (isCtrl)
            {
                controllerPeer = offlineCtrlPeer;
            }
        });
    }

    @Override
    public @Nullable Node getLocalNode()
    {
        return localNodeName == null ? null : (Node) nodesMap.get(localNodeName);
    }

    @Override
    public Peer getControllerPeer()
    {
        return controllerPeer;
    }

    @Override
    public @Nullable NodeName getLocalNodeName()
    {
        return localNodeName;
    }

    @Override
    public void setControllerPeer(
        @Nullable UUID ctrlUuidRef,
        Peer controllerPeerRef,
        UUID nodeUuid,
        String nodeName
    )
    {
        try
        {
            reconfigurationLock.writeLock().lock();
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();

            if (!controllerPeer.equals(controllerPeerRef))
            {
                // additional check for ctrlUuid in order to prevent "OtherController" response because of multiple
                // connections due to network-hicups. Unused connections will be closed soon anyways
                if (!Objects.equals(ctrlUuidRef, ctrlUuid))
                {
                    controllerPeer.sendMessage(
                        commonSerializer.onewayBuilder(InternalApiConsts.API_OTHER_CONTROLLER).build(),
                        InternalApiConsts.API_OTHER_CONTROLLER
                    );
                }
                else
                {
                    errorReporter.logDebug(
                        "Not sending '%s' since the same controller connected again",
                        InternalApiConsts.API_OTHER_CONTROLLER
                    );
                }
                // If we don't actively close the connection here, it will take some time(or forever)
                // to close it and the garbage collector starting freeing the objects
                controllerPeer.closeConnection();
            }
            else
            {
                // repeated authentication over the current connection (e.g. the controller sent more than one
                // Auth during a reconnect). We must not close our only living controller connection.
                errorReporter.logDebug(
                    "Re-authentication over the current controller connection, keeping the connection open"
                );
            }

            ctrlUuid = ctrlUuidRef;
            controllerPeer = controllerPeerRef;
            if (errorReporter instanceof BaseErrorReporter baseErrRep)
            {
                // we do not update clusterId during disconnect to avoid races (disconnect listener triggering after new
                // controller already connected, for example). Any error that happens during a reconnect will most
                // likely be due to the old controller. Also it is already very rare that a satellite changes controller
                // in the first place.
                baseErrRep.setClusterId(ctrlUuidRef == null ? null : ctrlUuidRef.toString());
            }

            Node localNode;
            try
            {
                localNodeName = new NodeName(nodeName);

                StltApiCallHandlerUtils stltUtils = stltApiCallHandlerUtils.get();
                stltUtils.clearCoreMaps();
                stltUtils.clearCaches();

                localNode = nodeFactory.getInstanceSatellite(
                    nodeUuid,
                    localNodeName,
                    Node.Type.SATELLITE,
                    new Node.Flags[] {}
                );

                nodesMap.put(localNode.getName(), localNode);
                setControllerPeerToCurrentLocalNode();

                transMgrProvider.get().commit();
            }
            catch (ImplementationError | TransactionException | InvalidNameException exc)
            {
                errorReporter.reportError(exc);
            }
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
            reconfigurationLock.writeLock().unlock();
        }
    }

    @Override
    public void setControllerPeerToCurrentLocalNode()
    {
        reconfigurationLock.readLock().lock();
        nodesMapLock.readLock().lock();
        try
        {
            nodesMap.get(localNodeName).setPeer(controllerPeer);
            /*
             * Initialize local node's extToolsList so that methods in the server project can also use the
             * localNode.get...getExtToolsMgr to check for external tools versions and support (like the
             * LuksLayerSizeCalculator does)
             */
            // we can also use the cached versions, since authentication re-caches everything, that should be good
            // enough for us
            controllerPeer.getExtToolsManager()
                .updateExternalToolsInfo(
                    stltExtToolsChecker.getExternalTools(false).values());
        }
        finally
        {
            nodesMapLock.readLock().unlock();
            reconfigurationLock.readLock().unlock();
        }
    }
}
