package com.linbit.linstor.core;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
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
    }

    public CtrlStltSerializer getInterComSerializer() {
        return interComSerializer;
    }

    public void applyFullSync(Set<NodePojo> nodes, Set<StorPoolPojo> storPools, Set<RscPojo> resources)
    {
        try
        {
            satellite.reconfigurationLock.writeLock().lock();
            satellite.nodesMapLock.writeLock().lock();
            satellite.rscDfnMapLock.writeLock().lock();
            satellite.storPoolDfnMapLock.writeLock().lock();

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
                Map<ResourceName, UUID> updatedResources = new TreeMap<>();
                Iterator<Resource> rscIter = localNode.iterateResources(apiCtx);
                while (rscIter.hasNext())
                {
                    Resource curRsc = rscIter.next();
                    updatedResources.put(curRsc.getDefinition().getName(), curRsc.getUuid());
                }
                DeviceManager deviceManager = satellite.getDeviceManager();
                StltUpdateTracker updTracker = deviceManager.getUpdateTracker();
                updTracker.checkMultipleResources(updatedResources);
            }
            else
            {
                satellite.getErrorReporter().logWarning(
                    "No node object that represents this satellite was received from the controller"
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
        finally
        {
            satellite.storPoolDfnMapLock.writeLock().unlock();
            satellite.rscDfnMapLock.writeLock().unlock();
            satellite.nodesMapLock.writeLock().unlock();
            satellite.reconfigurationLock.writeLock().unlock();
        }
    }

    public void requestNodeUpdate(UUID nodeUuid, NodeName nodeName)
    {
        sendRequest(
            satellite.getApiCallHandler().getInterComSerializer()
                .builder(InternalApiConsts.API_REQUEST_NODE, 0)
                .requestNodeUpdate(nodeUuid, nodeName.getDisplayName())
                .build()
        );
    }

    public void requestRscDfnUpate(UUID rscDfnUuid, ResourceName rscName)
    {
        sendRequest(
            satellite.getApiCallHandler().getInterComSerializer()
                .builder(InternalApiConsts.API_REQUEST_RSC_DFN, 0)
                .requestResourceDfnUpdate(rscDfnUuid, rscName.getDisplayName())
                .build()
        );
    }

    public void requestRscUpdate(UUID rscUuid, NodeName nodeName, ResourceName rscName)
    {
        sendRequest(
            satellite.getApiCallHandler().getInterComSerializer()
                .builder(InternalApiConsts.API_REQUEST_RSC, 0)
                .requestResourceUpdate(rscUuid, nodeName.getDisplayName(), rscName.getDisplayName())
                .build()
        );
    }

    public void requestStorPoolUpdate(UUID storPoolUuid, StorPoolName storPoolName)
    {
        sendRequest(
            satellite.getApiCallHandler().getInterComSerializer()
                .builder(InternalApiConsts.API_REQUEST_STOR_POOL, 0)
                .requestStoragePoolUpdate(storPoolUuid, storPoolName.getDisplayName())
                .build()
        );
    }

    public void applyNodeChanges(NodePojo nodePojo)
    {
        try
        {
            satellite.reconfigurationLock.writeLock().lock();
            nodeHandler.applyChanges(nodePojo);
        }
        catch (ImplementationError | Exception exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
        finally
        {
            satellite.reconfigurationLock.writeLock().unlock();
        }
    }

    public void deployResource(RscPojo rscRawData)
    {
        try
        {
            satellite.reconfigurationLock.writeLock().lock();
            // TODO: acquire nodesMapLock and rscDfnMapLock
            rscHandler.applyChanges(rscRawData);
        }
        catch (ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
        finally
        {
            satellite.reconfigurationLock.writeLock().unlock();
        }
    }

    public void deployStorPool(StorPoolPojo storPoolRaw)
    {
        try
        {
            satellite.reconfigurationLock.writeLock().lock();
            // TODO: acquire nodesMapLock and storPoolDfnMapLock
            storPoolHandler.applyChanges(storPoolRaw);
        }
        catch (ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
        finally
        {
            satellite.reconfigurationLock.writeLock().unlock();
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

    private void sendRequest(byte[] requestData)
    {
        try
        {
            if (requestData != null && requestData.length > 0)
            {
                Peer controllerPeer = satellite.getLocalNode().getPeer(apiCtx);
                Message message = controllerPeer.createMessage();
                message.setData(requestData);
                controllerPeer.sendMessage(message);
            }
            else
            {
                satellite.getErrorReporter().reportError(
                    new ImplementationError(
                        "Failed to serialize a request ",
                        null
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "StltApiCtx does not have enough privileges to send a message to controller",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegaMessageStateExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "StltApi was not able to send a message to controller due to an implementation error",
                    illegaMessageStateExc
                )
            );
        }
    }
}
