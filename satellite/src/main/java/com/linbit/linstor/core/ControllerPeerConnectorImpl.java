package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeSatelliteFactory;
import com.linbit.linstor.core.objects.StorPoolDefinitionSatelliteFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class ControllerPeerConnectorImpl implements ControllerPeerConnector
{
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    private final ErrorReporter errorReporter;

    private final AccessContext sysCtx;

    private final NodeSatelliteFactory nodeFactory;

    private final Provider<TransactionMgr> transMgrProvider;
    private final CommonSerializer commonSerializer;

    // Local NodeName received from the currently active controller
    private NodeName localNodeName;

    // The currently connected controller peer (can be null)
    private Peer controllerPeer;

    @Inject
    public ControllerPeerConnectorImpl(
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        StorPoolDefinitionSatelliteFactory storPoolDefinitionFactoryRef,
        NodeSatelliteFactory nodeFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CommonSerializer commonSerializerRef
    )
    {
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        nodeFactory = nodeFactoryRef;
        transMgrProvider = transMgrProviderRef;
        commonSerializer = commonSerializerRef;
    }

    @Override
    public Node getLocalNode()
    {
        return localNodeName == null ? null : (Node) nodesMap.get(localNodeName);
    }

    @Override
    public Peer getControllerPeer()
    {
        return controllerPeer;
    }

    @Override
    public NodeName getLocalNodeName()
    {
        return localNodeName;
    }

    @Override
    public void setControllerPeer(Peer controllerPeerRef, UUID nodeUuid, String nodeName)
    {
        try
        {
            reconfigurationLock.writeLock().lock();
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();

            if (controllerPeer != null)
            {
                controllerPeer.sendMessage(
                    commonSerializer.onewayBuilder(InternalApiConsts.API_OTHER_CONTROLLER).build(),
                    InternalApiConsts.API_OTHER_CONTROLLER
                );
                // no need to close connection, controller will do so when it finished processing the OUTDATED message
            }
            controllerPeer = controllerPeerRef;

            AccessContext tmpCtx = sysCtx.clone();
            tmpCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            Node localNode;
            try
            {
                localNodeName = new NodeName(nodeName);

                localNode = nodeFactory.getInstanceSatellite(
                    sysCtx,
                    nodeUuid,
                    localNodeName,
                    Node.Type.SATELLITE,
                    new Node.Flags[] {}
                );

                nodesMap.clear();
                rscDfnMap.clear();
                storPoolDfnMap.clear();
                // TODO: make sure everything is cleared

                nodesMap.put(localNode.getName(), localNode);
                setControllerPeerToCurrentLocalNode();

                transMgrProvider.get().commit();
            }
            catch (ImplementationError | TransactionException | InvalidNameException exc)
            {
                errorReporter.reportError(exc);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(
                "sysCtx does not have enough privileges to call node.setPeer",
                accDeniedExc
            );
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
            nodesMap.get(localNodeName).setPeer(sysCtx, controllerPeer);
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(exc);
        }
        finally
        {
            nodesMapLock.readLock().unlock();
            reconfigurationLock.readLock().unlock();
        }
    }
}
