package com.linbit.linstor.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.interfaces.serializer.StltRequestSerializer;
import com.linbit.linstor.api.interfaces.serializer.StltResourceRequestSerializer;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.satellite.serializer.GenericRequestSerializerProto;
import com.linbit.linstor.api.protobuf.satellite.serializer.ResourceRequestSerializerProto;
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

    private final StltRequestSerializer<NodeName> nodeRequestSerializer;
    private final StltRequestSerializer<ResourceName> rscDfnRequestSerializer;
    private final StltResourceRequestSerializer rscRequestSerializer;
    private final StltRequestSerializer<StorPoolName> storPoolRequestSerializer;

    private final AccessContext apiCtx;

    public StltApiCallHandler(Satellite satelliteRef, ApiType apiType, AccessContext apiCtx)
    {
        satellite = satelliteRef;
        this.apiCtx = apiCtx;
        ErrorReporter errorReporter = satelliteRef.getErrorReporter();
        switch (apiType)
        {
            case PROTOBUF:
                nodeRequestSerializer = new GenericRequestSerializerProto<>(
                    errorReporter,
                    InternalApiConsts.API_REQUEST_NODE
                );
                rscDfnRequestSerializer = new GenericRequestSerializerProto<>(
                    errorReporter,
                    InternalApiConsts.API_REQUEST_RSC_DFN
                );
                rscRequestSerializer = new ResourceRequestSerializerProto(errorReporter);
                storPoolRequestSerializer = new GenericRequestSerializerProto<>(
                    errorReporter,
                    InternalApiConsts.API_REQUEST_STOR_POOL
                );
                break;
            default:
                throw new ImplementationError("Unknown ApiType: " + apiType, null);
        }

        nodeHandler = new StltNodeApiCallHandler(satelliteRef, apiCtx, nodeRequestSerializer);
        rscDfnHandler = new StltRscDfnApiCallHandler(satelliteRef, apiCtx);
        rscHandler = new StltRscApiCallHandler(satelliteRef, apiCtx);
        storPoolHandler = new StltStorPoolApiCallHandler(satelliteRef, apiCtx);
    }


    public void applyFullSync(Set<NodePojo> nodes, Set<StorPoolPojo> storPools, Set<RscPojo> resources)
    {
        try
        {
            satellite.reconfigurationLock.writeLock().lock();

            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            for (NodePojo node : nodes)
            {
                nodeHandler.applyChanges(node, transMgr);
            }
            for (StorPoolPojo storPool : storPools)
            {
                storPoolHandler.applyChanges(storPool, transMgr);
            }
            for (RscPojo rsc : resources)
            {
                rscHandler.applyChanges(rsc, transMgr);
            }

            transMgr.commit();

            Set<NodeName> updatedNodeSet = new TreeSet<>();
            Set<StorPoolName> updatedStorPools = new TreeSet<>();
            Map<ResourceName, Set<NodeName>> updatedRscMap = new TreeMap<>();
            for (NodePojo node : nodes)
            {
                updatedNodeSet.add(new NodeName(node.getName()));
                satellite.getErrorReporter().logInfo("Node '" + node.getName() + "' created.");
            }
            for (StorPoolPojo storPool : storPools)
            {
                updatedStorPools.add(new StorPoolName(storPool.getStorPoolName()));
                satellite.getErrorReporter().logInfo("StorPool '" + storPool.getStorPoolName() + "' created.");
            }
            for (RscPojo rsc : resources)
            {
                Set<NodeName> nodeSet = new HashSet<>();
                nodeSet.add(satellite.getLocalNode().getName());
                for (OtherRscPojo otherRsc : rsc.getOtherRscList())
                {
                    nodeSet.add(new NodeName(otherRsc.getNodeName()));
                }
                updatedRscMap.put(
                    new ResourceName(rsc.getName()),
                    nodeSet
                );
                satellite.getErrorReporter().logInfo("Resource '" + rsc.getName() + "' created.");
            }
            satellite.getErrorReporter().logInfo("Full sync completed");

            DeviceManager deviceManager = satellite.getDeviceManager();
            deviceManager.nodeUpdateApplied(updatedNodeSet);
            deviceManager.storPoolUpdateApplied(updatedStorPools);
            deviceManager.rscUpdateApplied(updatedRscMap);
        }
        catch (Exception | ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
        finally
        {
            satellite.reconfigurationLock.writeLock().unlock();
        }
    }

    public void requestNodeUpdate(UUID nodeUuid, NodeName nodeName)
    {
        sendRequest(
            nodeRequestSerializer.getRequestMessage(
                0,
                nodeUuid,
                nodeName
            )
        );
    }

    public void requestRscDfnUpate(UUID rscDfnUuid, ResourceName rscName)
    {
        sendRequest(
            rscDfnRequestSerializer.getRequestMessage(
                0,
                rscDfnUuid,
                rscName
            )
        );
    }

    public void requestRscUpdate(UUID rscUuid, NodeName nodeName, ResourceName rscName)
    {
        sendRequest(
            rscRequestSerializer.getRequestMessage(
                0,
                rscUuid,
                nodeName,
                rscName
            )
        );
    }

    public void requestStorPoolUpdate(UUID storPoolUuid, StorPoolName storPoolName)
    {
        sendRequest(
            storPoolRequestSerializer.getRequestMessage(
                0,
                storPoolUuid,
                storPoolName
            )
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
