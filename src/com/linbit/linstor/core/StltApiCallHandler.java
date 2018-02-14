package com.linbit.linstor.core;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class StltApiCallHandler
{
    private final Satellite satellite;

    private final StltNodeApiCallHandler nodeHandler;
    private final StltRscDfnApiCallHandler rscDfnHandler;
    private final StltRscApiCallHandler rscHandler;
    private final StltStorPoolApiCallHandler storPoolHandler;

    private final AccessContext apiCtx;

    private final CtrlStltSerializer interComSerializer;

    private final TreeMap<Long, ApplyData> dataToApply;

    public StltApiCallHandler(Satellite satelliteRef, ApiType apiType, AccessContext apiCtxRef)
    {
        satellite = satelliteRef;
        apiCtx = apiCtxRef;
        ErrorReporter errorReporter = satelliteRef.getErrorReporter();
        switch (apiType)
        {
            case PROTOBUF:
                interComSerializer = new ProtoCtrlStltSerializer(errorReporter, apiCtxRef);
                break;
            default:
                throw new ImplementationError("Unknown ApiType: " + apiType, null);
        }

        nodeHandler = new StltNodeApiCallHandler(satelliteRef, apiCtx);
        rscDfnHandler = new StltRscDfnApiCallHandler(satelliteRef, apiCtx);
        rscHandler = new StltRscApiCallHandler(satelliteRef, apiCtx);
        storPoolHandler = new StltStorPoolApiCallHandler(satelliteRef, apiCtx);

        dataToApply = new TreeMap<>();
    }

    public CtrlStltSerializer getInterComSerializer()
    {
        return interComSerializer;
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
        final ExtCmd extCommand = new ExtCmd(satellite.getTimer(), satellite.getErrorReporter());
        String hostName = "";
        try
        {
            final ExtCmd.OutputData output = extCommand.exec("uname", "-n");
            final String stdOut = new String(output.stdoutData);
            hostName = stdOut.trim();
        }
        catch (ChildProcessTimeoutException ex)
        {
            satellite.getErrorReporter().reportError(ex);
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

            satellite.getErrorReporter().logError(cause);
        }
        else
        {
            synchronized (dataToApply)
            {
                dataToApply.clear(); // controller should not have sent us anything before the authentication.
                // that means, everything in this map is out-dated data + we should receive a full sync next.
            }

            satellite.setControllerPeer(
                controllerPeer,
                nodeUuid,
                nodeName,
                disklessStorPoolDfnUuid,
                disklessStorPoolUuid
            );
            satellite.getErrorReporter().logInfo("Controller connected and authenticated");
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
            satellite.reconfigurationLock.writeLock().lock();
            satellite.nodesMapLock.writeLock().lock();
            satellite.rscDfnMapLock.writeLock().lock();
            satellite.storPoolDfnMapLock.writeLock().lock();

            if (satellite.getCurrentFullSyncId() == fullSyncId)
            {
                // only apply this fullSync if it is newer than the last one

                // clear all data
                satellite.nodesMap.clear();
                satellite.rscDfnMap.clear();
                satellite.storPoolDfnMap.clear();

                SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
                for (NodePojo node : nodes)
                {
                    Node curNode = nodeHandler.applyChanges(node, transMgr);
                    if (curNode != null)
                    {
                        satellite.nodesMap.put(curNode.getName(), curNode);
                    }
                }
                satellite.setControllerPeerToCurrentLocalNode();

                for (StorPoolPojo storPool : storPools)
                {
                    storPoolHandler.applyChanges(storPool, transMgr);
                }

                for (RscPojo rsc : resources)
                {
                    rscHandler.applyChanges(rsc, transMgr);
                }

                transMgr.commit();

                for (NodePojo node : nodes)
                {
                    satellite.getErrorReporter().logTrace("Node '" + node.getName() + "' received from Controller.");
                }
                for (StorPoolPojo storPool : storPools)
                {
                    satellite.getErrorReporter().logTrace(
                        "StorPool '" + storPool.getStorPoolName() + "' received from Controller."
                    );
                }
                for (RscPojo rsc : resources)
                {
                    satellite.getErrorReporter().logTrace("Resource '" + rsc.getName() + "' created.");
                }
                satellite.getErrorReporter().logTrace("Full sync with controller finished");

                // Atomically notify the DeviceManager to check all resources
                Node localNode = satellite.getLocalNode();
                if (localNode != null)
                {
                    DeviceManager devMgr = satellite.getDeviceManager();
                    if (devMgr != null)
                    {
                        devMgr.fullSyncApplied();
                    }
                }
                else
                {
                    satellite.getErrorReporter().logWarning(
                        "No node object that represents this satellite was received from the controller"
                    );
                }

                satellite.setFullSyncApplied();
            }
            else
            {
                satellite.getErrorReporter().logWarning(
                    "Ignored an incoming but outdated fullsync"
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);

            Peer controllerPeer = satellite.getControllerPeer();
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
            satellite.getNextFullSyncId();

        }
        finally
        {
            satellite.storPoolDfnMapLock.writeLock().unlock();
            satellite.rscDfnMapLock.writeLock().unlock();
            satellite.nodesMapLock.writeLock().unlock();
            satellite.reconfigurationLock.writeLock().unlock();
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
                satellite.reconfigurationLock.readLock().lock();
                if (data.getFullSyncId() == satellite.getCurrentFullSyncId())
                {
                    try
                    {
                        ApplyData overriddenData = dataToApply.put(data.getUpdateId(), data);
                        if (overriddenData != null)
                        {
                            satellite.getErrorReporter().reportError(
                                new ImplementationError(
                                    "We have overridden data which we did not update yet.",
                                    null
                                    )
                                );
                            satellite.shutdown(apiCtx); // critical error. shutdown and fix this implementation error
                        }

                        Entry<Long, ApplyData> nextEntry;
                        nextEntry = dataToApply.firstEntry();
                        while (
                            nextEntry != null &&
                            nextEntry.getKey() == satellite.getCurrentAwaitedUpdateId()
                        )
                        {
                            nextEntry.getValue().applyChange();
                            dataToApply.remove(nextEntry.getKey());
                            satellite.awaitedUpdateApplied();

                            nextEntry = dataToApply.firstEntry();
                        }
                    }
                    catch (ImplementationError | Exception exc)
                    {
                        satellite.getErrorReporter().reportError(exc);
                        try
                        {
                            satellite.getLocalNode().getPeer(apiCtx).closeConnection();
                            // there is nothing else we can safely do.
                            // skipping the update might cause data-corruption
                            // not skipping will queue the new data packets but will not apply those as the
                            // awaitedUpdateId will never increment.
                        }
                        catch (AccessDeniedException exc1)
                        {
                            satellite.getErrorReporter().reportError(new ImplementationError(exc));
                        }
                    }
                }
                else
                {
                    satellite.getErrorReporter().logWarning("Ignoring received outdated update. ");
                }
            }
            finally
            {
                satellite.reconfigurationLock.readLock().unlock();
            }
        }
    }

    public void handlePrimaryResource(
        Peer controllerPeer,
        int msgId,
        String rscNameStr,
        UUID rscUuid
    )
    {
        try
        {
            satellite.rscDfnMapLock.writeLock().lock();
            rscDfnHandler.primaryResource(rscNameStr, rscUuid);
        }
        finally
        {
            satellite.rscDfnMapLock.writeLock().unlock();
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
                satellite.nodesMapLock.writeLock().lock();

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
                satellite.nodesMapLock.writeLock().unlock();
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
                satellite.nodesMapLock.writeLock().lock();
                satellite.rscDfnMapLock.writeLock().lock();

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
                satellite.rscDfnMapLock.writeLock().unlock();
                satellite.nodesMapLock.writeLock().unlock();
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
                satellite.nodesMapLock.writeLock().lock();
                satellite.storPoolDfnMapLock.writeLock().lock();

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
                satellite.nodesMapLock.writeLock().unlock();
                satellite.storPoolDfnMapLock.writeLock().unlock();
            }
        }
    }
}
