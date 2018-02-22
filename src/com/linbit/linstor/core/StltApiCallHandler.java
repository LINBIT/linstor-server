package com.linbit.linstor.core;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.core.StltStorPoolApiCallHandler.ChangedData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.timer.CoreTimer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class StltApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;

    private final CoreTimer timer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final UpdateMonitor updateMonitor;
    private final DeviceManager deviceManager;
    private final ApplicationLifecycleManager applicationLifecycleManager;

    private final StltNodeApiCallHandler nodeHandler;
    private final StltRscDfnApiCallHandler rscDfnHandler;
    private final StltRscApiCallHandler rscHandler;
    private final StltStorPoolApiCallHandler storPoolHandler;

    private final CtrlStltSerializer interComSerializer;

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    private final TreeMap<Long, ApplyData> dataToApply;

    @Inject
    public StltApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CoreTimer timerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        UpdateMonitor updateMonitorRef,
        DeviceManager deviceManagerRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        StltNodeApiCallHandler nodeHandlerRef,
        StltRscDfnApiCallHandler rscDfnHandlerRef,
        StltRscApiCallHandler rscHandlerRef,
        StltStorPoolApiCallHandler storPoolHandlerRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        timer = timerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        updateMonitor = updateMonitorRef;
        deviceManager = deviceManagerRef;
        applicationLifecycleManager = applicationLifecycleManagerRef;
        nodeHandler = nodeHandlerRef;
        rscDfnHandler = rscDfnHandlerRef;
        rscHandler = rscHandlerRef;
        storPoolHandler = storPoolHandlerRef;
        interComSerializer = interComSerializerRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;

        dataToApply = new TreeMap<>();
    }

    public ApiCallRcImpl authenticate(
        UUID nodeUuid,
        String nodeName,
        UUID disklessStorPoolDfnUuid,
        UUID disklessStorPoolUuid,
        Peer controllerPeer
    )
        throws IOException
    {
        ApiCallRcImpl apiCallRc = null;

        // get satellites current hostname
        final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
        String hostName = "";
        try
        {
            final ExtCmd.OutputData output = extCommand.exec("uname", "-n");
            final String stdOut = new String(output.stdoutData);
            hostName = stdOut.trim();
        }
        catch (ChildProcessTimeoutException ex)
        {
            errorReporter.reportError(ex);
        }

        // Check if satellite hostname is equal to the given nodename
        if (hostName == null || !hostName.toLowerCase().equals(nodeName))
        {
            ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
            entry.setReturnCode(InternalApiConsts.API_AUTH_ERROR_HOST_MISMATCH);
            entry.setMessageFormat("Satellite node name doesn't match hostname.");
            String cause = String.format(
                "Satellite node name '%s' doesn't match nodes hostname '%s'.",
                nodeName,
                hostName
            );
            entry.setCauseFormat(cause);
            apiCallRc = new ApiCallRcImpl();
            apiCallRc.addEntry(entry);

            errorReporter.logError(cause);
        }
        else
        {
            synchronized (dataToApply)
            {
                dataToApply.clear(); // controller should not have sent us anything before the authentication.
                // that means, everything in this map is out-dated data + we should receive a full sync next.
            }

            controllerPeerConnector.setControllerPeer(
                controllerPeer,
                nodeUuid,
                nodeName,
                disklessStorPoolDfnUuid,
                disklessStorPoolUuid
            );
            errorReporter.logInfo("Controller connected and authenticated");
        }

        return apiCallRc;
    }

    public void applyFullSync(
        Set<NodePojo> nodes,
        Set<StorPoolPojo> storPools,
        Set<RscPojo> resources,
        long fullSyncId
    )
    {
        try
        {
            reconfigurationLock.writeLock().lock();
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();

            if (updateMonitor.getCurrentFullSyncId() == fullSyncId)
            {
                // only apply this fullSync if it is newer than the last one

                // clear all data
                nodesMap.clear();
                rscDfnMap.clear();
                storPoolDfnMap.clear();

                SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
                for (NodePojo node : nodes)
                {
                    Node curNode = nodeHandler.applyChanges(node, transMgr);
                    if (curNode != null)
                    {
                        nodesMap.put(curNode.getName(), curNode);
                    }
                }
                controllerPeerConnector.setControllerPeerToCurrentLocalNode();

                for (StorPoolPojo storPool : storPools)
                {
                    ChangedData appliedChanges = storPoolHandler.applyChanges(storPool, transMgr);
                    StorPoolDefinition storPoolDfnToRegister = appliedChanges.storPoolDfnToRegister;
                    if (storPoolDfnToRegister != null)
                    {
                        storPoolDfnMap.put(
                            storPoolDfnToRegister.getName(),
                            storPoolDfnToRegister
                        );
                    }
                }

                for (RscPojo rsc : resources)
                {
                    rscHandler.applyChanges(rsc, transMgr);
                }

                transMgr.commit();

                for (NodePojo node : nodes)
                {
                    errorReporter.logTrace("Node '" + node.getName() + "' received from Controller.");
                }
                for (StorPoolPojo storPool : storPools)
                {
                    errorReporter.logTrace(
                        "StorPool '" + storPool.getStorPoolName() + "' received from Controller."
                    );
                }
                for (RscPojo rsc : resources)
                {
                    errorReporter.logTrace("Resource '" + rsc.getName() + "' created.");
                }
                errorReporter.logTrace("Full sync with controller finished");

                // Atomically notify the DeviceManager to check all resources
                Node localNode = controllerPeerConnector.getLocalNode();
                if (localNode != null)
                {
                    if (deviceManager != null)
                    {
                        deviceManager.fullSyncApplied();
                    }
                }
                else
                {
                    errorReporter.logWarning(
                        "No node object that represents this satellite was received from the controller"
                    );
                }

                updateMonitor.setFullSyncApplied();
            }
            else
            {
                errorReporter.logWarning(
                    "Ignored an incoming but outdated fullsync"
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);

            Peer controllerPeer = controllerPeerConnector.getControllerPeer();
            controllerPeer.sendMessage(
                interComSerializer.builder(InternalApiConsts.API_FULL_SYNC_FAILED, 0)
                    .build()
            );

            // sending this message should tell the controller to not send us any further data, as
            // updates would be based on an invalid fullSync, and receiving this fullSync again
            // would most likely cause the same exception as now.

            // however, in order to avoid implementation errors of the controller, we additionally
            // increase the fullSyncId but not telling the controller about it.
            // even if the controller still sends us data, we will ignore them as they will look like
            // "out-dated" data.
            // when recreating the connection, and the controller is positive to send us an authentication
            // message, we will again increase the fullSyncId and expect the fullSync from the controller.

            // in other words: if this exception happens, either the controller or this satellite has
            // to drop the connection (e.g. restart) in order to re-enable applying fullSyncs.
            updateMonitor.getNextFullSyncId();

        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
            reconfigurationLock.writeLock().unlock();
        }
    }

    public void applyNodeChanges(NodePojo nodePojo)
    {
        applyChangedData(new ApplyNodeData(nodePojo));
    }

    public void applyDeletedNodeChange(String nodeName)
    {
        applyChangedData(new ApplyNodeData(nodeName));
    }

    public void applyResourceChanges(RscPojo rscRawData)
    {
        applyChangedData(new ApplyRscData(rscRawData));
    }

    public void applyDeletedResourceChange(String rscNameStr)
    {
        applyChangedData(new ApplyRscData(rscNameStr));
    }

    public void applyStorPoolChanges(StorPoolPojo storPoolRaw)
    {
        applyChangedData(new ApplyStorPoolData(storPoolRaw));
    }

    public void applyDeletedStorPoolChange(String storPoolNameStr)
    {
        applyChangedData(new ApplyStorPoolData(storPoolNameStr));
    }

    private void applyChangedData(ApplyData data)
    {
        synchronized (dataToApply)
        {
            try
            {
                reconfigurationLock.readLock().lock();
                if (data.getFullSyncId() == updateMonitor.getCurrentFullSyncId())
                {
                    try
                    {
                        ApplyData overriddenData = dataToApply.put(data.getUpdateId(), data);
                        if (overriddenData != null)
                        {
                            errorReporter.reportError(
                                new ImplementationError(
                                    "We have overridden data which we did not update yet.",
                                    null
                                )
                            );
                            // critical error. shutdown and fix this implementation error
                            applicationLifecycleManager.shutdown(apiCtx);
                        }

                        Entry<Long, ApplyData> nextEntry;
                        nextEntry = dataToApply.firstEntry();
                        while (
                            nextEntry != null &&
                            nextEntry.getKey() == updateMonitor.getCurrentAwaitedUpdateId()
                        )
                        {
                            nextEntry.getValue().applyChange();
                            dataToApply.remove(nextEntry.getKey());
                            updateMonitor.awaitedUpdateApplied();

                            nextEntry = dataToApply.firstEntry();
                        }
                    }
                    catch (ImplementationError | Exception exc)
                    {
                        errorReporter.reportError(exc);
                        try
                        {
                            controllerPeerConnector.getLocalNode().getPeer(apiCtx).closeConnection();
                            // there is nothing else we can safely do.
                            // skipping the update might cause data-corruption
                            // not skipping will queue the new data packets but will not apply those as the
                            // awaitedUpdateId will never increment.
                        }
                        catch (AccessDeniedException exc1)
                        {
                            errorReporter.reportError(new ImplementationError(exc));
                        }
                    }
                }
                else
                {
                    errorReporter.logWarning("Ignoring received outdated update. ");
                }
            }
            finally
            {
                reconfigurationLock.readLock().unlock();
            }
        }
    }

    public void handlePrimaryResource(
        String rscNameStr,
        UUID rscUuid
    )
    {
        try
        {
            rscDfnMapLock.writeLock().lock();
            rscDfnHandler.primaryResource(rscNameStr, rscUuid);
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
        }

    }

    private interface ApplyData
    {
        long getFullSyncId();
        long getUpdateId();

        void applyChange();
    }

    private class ApplyNodeData implements ApplyData
    {
        private NodePojo nodePojo;
        private String deletedNodeName;

        ApplyNodeData(NodePojo nodePojoRef)
        {
            nodePojo = nodePojoRef;
            deletedNodeName = null;
        }

        ApplyNodeData(String nodeNameRef)
        {
            nodePojo = null;
            deletedNodeName = nodeNameRef;
        }

        @Override
        public long getFullSyncId()
        {
            return nodePojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return nodePojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try
            {
                nodesMapLock.writeLock().lock();

                if (nodePojo != null)
                {
                    nodeHandler.applyChanges(nodePojo);
                }
                else
                {
                    nodeHandler.applyDeletedNode(deletedNodeName);
                }
            }
            finally
            {
                nodesMapLock.writeLock().unlock();
            }
        }
    }

    private class ApplyRscData implements ApplyData
    {
        private RscPojo rscPojo;
        private String deletedRscName;

        ApplyRscData(RscPojo rscPojoRef)
        {
            rscPojo = rscPojoRef;
        }

        ApplyRscData(String rscNameRef)
        {
            deletedRscName = rscNameRef;
        }

        @Override
        public long getFullSyncId()
        {
            return rscPojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return rscPojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try
            {
                nodesMapLock.writeLock().lock();
                rscDfnMapLock.writeLock().lock();

                if (rscPojo != null)
                {
                    rscHandler.applyChanges(rscPojo);
                }
                else
                {
                    rscHandler.applyDeletedRsc(deletedRscName);
                }
            }
            finally
            {
                rscDfnMapLock.writeLock().unlock();
                nodesMapLock.writeLock().unlock();
            }
        }
    }

    private class ApplyStorPoolData implements ApplyData
    {
        private StorPoolPojo storPoolPojo;
        private String deletedStorPoolName;

        ApplyStorPoolData(StorPoolPojo storPoolPojoRef)
        {
            storPoolPojo = storPoolPojoRef;
        }

        ApplyStorPoolData(String storPoolNameRef)
        {
            deletedStorPoolName = storPoolNameRef;
        }

        @Override
        public long getFullSyncId()
        {
            return storPoolPojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return storPoolPojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try
            {
                nodesMapLock.writeLock().lock();
                storPoolDfnMapLock.writeLock().lock();

                if (storPoolPojo != null)
                {
                    storPoolHandler.applyChanges(storPoolPojo);
                }
                else
                {
                    storPoolHandler.applyDeletedStorPool(deletedStorPoolName);
                }
            }
            finally
            {
                nodesMapLock.writeLock().unlock();
                storPoolDfnMapLock.writeLock().unlock();
            }
        }
    }
}
