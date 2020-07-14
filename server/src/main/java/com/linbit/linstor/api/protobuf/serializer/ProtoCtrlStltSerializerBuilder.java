package com.linbit.linstor.api.protobuf.serializer;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer.CommonSerializerBuilder;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer.CtrlStltSerializerBuilder;
import com.linbit.linstor.api.protobuf.ProtoStorPoolFreeSpaceUtils;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.common.StltConfigOuterClass.StltConfig;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;
import com.linbit.linstor.proto.javainternal.c2s.IntControllerOuterClass.IntController;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNetIf;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNetIf.Builder;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNode;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNodeConn;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntOtherRsc;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntRsc;
import com.linbit.linstor.proto.javainternal.c2s.IntSnapshotOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntSnapshotOuterClass.IntSnapshot;
import com.linbit.linstor.proto.javainternal.c2s.IntStorPoolOuterClass.IntStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgCreateDevicePoolOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.MsgCreateDevicePoolOuterClass.MsgCreateDevicePool;
import com.linbit.linstor.proto.javainternal.c2s.MsgDeleteDevicePoolOuterClass.MsgDeleteDevicePool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyControllerOuterClass.MsgIntApplyController;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedNodeOuterClass.MsgIntApplyDeletedNode;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedRscOuterClass.MsgIntApplyDeletedRsc;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedStorPoolOuterClass.MsgIntApplyDeletedStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyFullSyncOuterClass.MsgIntApplyFullSync;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyNodeOuterClass.MsgIntApplyNode;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyRscOuterClass.MsgIntApplyRsc;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplySnapshotOuterClass.MsgIntApplySnapshot;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyStorPoolOuterClass.MsgIntApplyStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntAuthOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntCryptKeyOuterClass.MsgIntCryptKey;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntSnapshotEndedDataOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.MsgReqPhysicalDevicesOuterClass.MsgReqPhysicalDevices;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyConfigResponseOuterClass.MsgIntApplyConfigResponse;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyNodeSuccessOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyRscSuccessOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyStorPoolSuccessOuterClass.MsgIntApplyStorPoolSuccess;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntPrimaryOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntSnapshotShippedOuterClass.MsgIntSnapshotShipped;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntUpdateFreeSpaceOuterClass.MsgIntUpdateFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgPhysicalDevicesOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgPhysicalDevicesOuterClass.MsgPhysicalDevices;
import com.linbit.linstor.proto.javainternal.s2c.MsgRscFailedOuterClass.MsgRscFailed;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.RaidLevel;
import com.linbit.utils.Base64;
import com.linbit.utils.Either;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import static java.util.stream.Collectors.toList;

public class ProtoCtrlStltSerializerBuilder extends ProtoCommonSerializerBuilder
    implements CtrlStltSerializer.CtrlStltSerializerBuilder
{
    private final CtrlSerializerHelper ctrlSerializerHelper;
    private final ResourceSerializerHelper rscSerializerHelper;
    private final SnapshotSerializerHelper snapshotSerializerHelper;
    private final NodeSerializerHelper nodeSerializerHelper;
    private final CtrlSecurityObjects secObjs;

    public ProtoCtrlStltSerializerBuilder(
        ErrorReporter errReporter,
        AccessContext serializerCtx,
        CtrlSecurityObjects secObjsRef,
        Props ctrlConfRef,
        final String apiCall,
        Long apiCallId,
        boolean isAnswer
    )
    {
        super(errReporter, serializerCtx, apiCall, apiCallId, isAnswer);
        secObjs = secObjsRef;

        ctrlSerializerHelper = new CtrlSerializerHelper(ctrlConfRef);
        rscSerializerHelper = new ResourceSerializerHelper();
        snapshotSerializerHelper = new SnapshotSerializerHelper();
        nodeSerializerHelper = new NodeSerializerHelper();
    }

    /*
     * Controller -> Satellite
     */
    @Override
    public ProtoCtrlStltSerializerBuilder authMessage(UUID nodeUuid, String nodeName, byte[] sharedSecret)
    {
        try
        {
            MsgIntAuthOuterClass.MsgIntAuth.newBuilder()
                .setNodeUuid(nodeUuid.toString())
                .setNodeName(nodeName)
                .setSharedSecret(ByteString.copyFrom(sharedSecret))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder changedConfig(com.linbit.linstor.core.cfg.StltConfig stltConfig)
        throws IOException
    {
        StltConfig.Builder bld = stltConfig(
            stltConfig.getConfigDir(),
            stltConfig.isDebugConsoleEnabled(),
            stltConfig.isLogPrintStackTrace(),
            stltConfig.getLogDirectory(),
            stltConfig.getLogLevel(),
            stltConfig.getLogLevelLinstor(),
            stltConfig.getStltOverrideNodeName(),
            stltConfig.isOpenflex(),
            stltConfig.getDrbdKeepResPattern(),
            stltConfig.getNetBindAddress(),
            stltConfig.getNetPort(),
            stltConfig.getNetType()
        );
        bld.build().writeDelimitedTo(baos);
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder stltConfigApplied(boolean success) throws IOException
    {
        MsgIntApplyConfigResponse.newBuilder().setSuccess(success).build().writeDelimitedTo(baos);
        return this;
    }

    // no fullSync- or update-id needed
    @Override
    public ProtoCtrlStltSerializerBuilder changedNode(UUID nodeUuid, String nodeName)
    {
        appendObjectId(nodeUuid, nodeName);
        return this;
    }

    // no fullSync- or update-id needed
    @Override
    public ProtoCtrlStltSerializerBuilder changedResource(UUID rscUuid, String rscName)
    {
        appendObjectId(rscUuid, rscName);
        return this;
    }

    // no fullSync- or update-id needed
    @Override
    public ProtoCtrlStltSerializerBuilder changedStorPool(UUID storPoolUuid, String storPoolName)
    {
        appendObjectId(storPoolUuid, storPoolName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder changedSnapshot(
        String rscName,
        UUID snapshotUuid,
        String snapshotName
    )
    {
        appendObjectId(null, rscName);
        appendObjectId(snapshotUuid, snapshotName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder controllerData(
        long fullSyncTimestamp,
        long serializerid
    )
    {
        try
        {
            ctrlSerializerHelper
                .buildApplyControllerMsg(fullSyncTimestamp, serializerid)
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder node(
        Node node,
        Collection<Node> relatedNodes,
        long fullSyncTimestamp,
        long serializerId
    )
    {
        try
        {
            MsgIntApplyNode.newBuilder()
                .setNode(nodeSerializerHelper.buildNodeMsg(node, relatedNodes))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(serializerId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder deletedNode(
        String nodeNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyDeletedNode.newBuilder()
                .setNodeName(nodeNameStr)
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder resource(
        Resource localResource,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyRsc.newBuilder()
                .setRsc(rscSerializerHelper.buildIntResource(localResource))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder deletedResource(
        String rscNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyDeletedRsc.newBuilder()
                .setRscName(rscNameStr)
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder storPool(
        StorPool storPool,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyStorPool.newBuilder()
                .setStorPool(buildIntStorPoolMsg(storPool))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder deletedStorPool(
        String storPoolNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyDeletedStorPool.newBuilder()
                .setStorPoolName(storPoolNameStr)
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder snapshot(
        Snapshot snapshot, long fullSyncId, long updateId
    )
    {
        try
        {
            MsgIntApplySnapshot.newBuilder()
                .setSnapshot(snapshotSerializerHelper.buildSnapshotMsg(snapshot))
                .setFullSyncId(fullSyncId)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder endedSnapshot(
        String resourceNameStr, String snapshotNameStr, long fullSyncId, long updateId
    )
    {
        try
        {
            MsgIntSnapshotEndedDataOuterClass.MsgIntSnapshotEndedData.newBuilder()
                .setRscName(resourceNameStr)
                .setSnapshotName(snapshotNameStr)
                .setFullSyncId(fullSyncId)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder fullSync(
        Set<Node> nodeSet,
        Set<StorPool> storPools,
        Set<Resource> resources,
        Set<Snapshot> snapshots,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            ArrayList<IntNode> serializedNodes = new ArrayList<>();
            ArrayList<IntStorPool> serializedStorPools = new ArrayList<>();
            ArrayList<IntRsc> serializedRscs = new ArrayList<>();
            ArrayList<IntSnapshot> serializedSnapshots = new ArrayList<>();

            IntController serializedCtrl = ctrlSerializerHelper.buildControllerDataMsg();

            LinkedList<Node> nodes = new LinkedList<>(nodeSet);

            while (!nodes.isEmpty())
            {
                Node node = nodes.removeFirst();
                serializedNodes.add(
                    nodeSerializerHelper.buildNodeMsg(node, nodes)
                );
            }
            for (StorPool storPool : storPools)
            {
                serializedStorPools.add(
                    buildIntStorPoolMsg(storPool)
                );
            }
            for (Resource rsc : resources)
            {
                if (rsc.iterateVolumes().hasNext())
                {
                    serializedRscs.add(rscSerializerHelper.buildIntResource(rsc));
                }
            }
            for (Snapshot snapshot : snapshots)
            {
                serializedSnapshots.add(snapshotSerializerHelper.buildSnapshotMsg(snapshot));
            }

            String encodedMasterKey = "";
            byte[] cryptKey = secObjs.getCryptKey();
            if (cryptKey != null)
            {
                encodedMasterKey = Base64.encode(cryptKey);
            }
            MsgIntApplyFullSync.newBuilder()
                .addAllNodes(serializedNodes)
                .addAllStorPools(serializedStorPools)
                .addAllRscs(serializedRscs)
                .addAllSnapshots(serializedSnapshots)
                .setFullSyncTimestamp(fullSyncTimestamp)
                .setMasterKey(encodedMasterKey)
                .setCtrl(serializedCtrl)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    /*
     * Satellite -> Controller
     */
    @Override
    public ProtoCtrlStltSerializerBuilder primaryRequest(String rscName, String rscUuid, boolean alreadyInitialized)
    {
        try
        {
            MsgIntPrimaryOuterClass.MsgIntPrimary.newBuilder()
                .setRscName(rscName)
                .setRscUuid(rscUuid)
                .setAlreadyInitialized(alreadyInitialized)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder notifyNodeApplied(Node node)
    {
        try
        {
            MsgIntApplyNodeSuccessOuterClass.MsgIntApplyNodeSuccess.newBuilder()
                .setNodeId(
                    IntObjectId.newBuilder()
                        .setUuid(node.getUuid().toString())
                        .setName(node.getName().displayValue)
                        .build()
                )
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder notifyResourceApplied(
        Resource resource,
        Map<StorPool, SpaceInfo> freeSpaceMap
    )
    {
        try
        {
            MsgIntApplyRscSuccessOuterClass.MsgIntApplyRscSuccess.newBuilder()
                .setRscId(
                    IntObjectId.newBuilder()
                        .setUuid(resource.getUuid().toString())
                        .setName(resource.getDefinition().getName().displayValue)
                        .build()
                )
                .addAllFreeSpace(
                    ProtoStorPoolFreeSpaceUtils.getAllStorPoolFreeSpaces(freeSpaceMap)
                )
                .setLayerObject(
                    ProtoCommonSerializerBuilder.LayerObjectSerializer.serializeLayerObject(
                        resource.getLayerData(serializerCtx),
                        serializerCtx
                    )
                )
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifySnapshotShipped(Snapshot snapRef, boolean success)
    {
        try
        {
            MsgIntSnapshotShipped.newBuilder()
                .setRscName(snapRef.getResourceName().displayValue)
                .setSnapName(snapRef.getSnapshotName().displayValue)
                .setSuccess(success)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder notifyResourceFailed(Resource resource, ApiCallRc apiCallRc)
    {
        try
        {
            MsgRscFailed.newBuilder()
                .setRsc(ProtoCommonSerializerBuilder.serializeResource(serializerCtx, resource))
                .addAllResponses(ProtoCommonSerializerBuilder.serializeApiCallRc(apiCallRc))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestNodeUpdate(UUID nodeUuid, String nodeName)
    {
        appendObjectId(nodeUuid, nodeName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestControllerUpdate()
    {
        return requestNodeUpdate(UUID.randomUUID(), "thisnamedoesnotmatter");
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName)
    {
        appendObjectId(null, nodeName);
        appendObjectId(rscUuid, rscName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName)
    {
        appendObjectId(storPoolUuid, storPoolName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestSnapshotUpdate(
        String rscName, UUID snapshotUuid, String snapshotName
    )
    {
        appendObjectId(null, rscName);
        appendObjectId(snapshotUuid, snapshotName);
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder updateFreeCapacities(
        Map<StorPool, SpaceInfo> spaceInfoMap
    )
    {
        try
        {
            List<StorPoolFreeSpace> freeSpaces = new ArrayList<>();
            for (Entry<StorPool, SpaceInfo> entry : spaceInfoMap.entrySet())
            {
                StorPool storPool = entry.getKey();
                SpaceInfo spaceInfo = entry.getValue();
                freeSpaces.add(
                    StorPoolFreeSpaceOuterClass.StorPoolFreeSpace.newBuilder()
                        .setStorPoolUuid(storPool.getUuid().toString())
                        .setStorPoolName(storPool.getName().displayValue)
                        .setFreeCapacity(spaceInfo.freeCapacity)
                        .setTotalCapacity(spaceInfo.totalCapacity)
                        .build()
                );
            }


            MsgIntUpdateFreeSpace.newBuilder()
                .addAllFreeSpace(freeSpaces)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder storPoolApplied(
        StorPool storPool,
        SpaceInfo spaceInfo,
        boolean supportsSnapshotsRef
    )
    {
        try
        {
            MsgIntApplyStorPoolSuccess.newBuilder()
                .setStorPoolName(storPool.getName().displayValue)
                .setFreeSpace(
                    StorPoolFreeSpaceOuterClass.StorPoolFreeSpace.newBuilder()
                        .setStorPoolUuid(storPool.getUuid().toString())
                        .setStorPoolName(storPool.getName().displayValue)
                        .setFreeCapacity(spaceInfo.freeCapacity)
                        .setTotalCapacity(spaceInfo.totalCapacity)
                        .build()
                    )
                .setSupportsSnapshots(supportsSnapshotsRef)
                .setIsPmem(storPool.isPmem())
                .setIsVdo(storPool.isVDO())
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;

    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder requestPhysicalDevices(boolean filter)
    {
        try
        {
            MsgReqPhysicalDevices.newBuilder()
                .setFilter(filter)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder createDevicePool(
        List<String> devicePaths,
        DeviceProviderKind providerKindRef,
        RaidLevel raidLevel,
        String poolName,
        boolean vdoEnabled,
        long vdoLogicalSizeKib,
        long vdoSlabSize
    )
    {
        try
        {
            MsgCreateDevicePool.Builder msgCreateDevicePoolBuilder =
            MsgCreateDevicePool.newBuilder()
                .addAllDevicePaths(devicePaths)
                .setProviderKind(asProviderType(providerKindRef))
                .setRaidLevel(MsgCreateDevicePool.RaidLevel.valueOf(raidLevel.name()))
                .setPoolName(poolName)
                .setLogicalSizeKib(vdoLogicalSizeKib);

            if (vdoEnabled)
            {
                msgCreateDevicePoolBuilder.setVdoArguments(
                    MsgCreateDevicePoolOuterClass.VdoArguments.newBuilder()
                        .setSlabSizeKib(vdoSlabSize)
                        .build()
                );
            }

            msgCreateDevicePoolBuilder
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder deleteDevicePool(
        List<String> devicePaths,
        DeviceProviderKind providerKindRef,
        String poolName
    )
    {
        try
        {
            MsgDeleteDevicePool.Builder msgDeleteDevicePoolBuilder =
                MsgDeleteDevicePool.newBuilder()
                    .addAllDevicePaths(devicePaths)
                    .setProviderKind(asProviderType(providerKindRef))
                    .setPoolName(poolName);

            msgDeleteDevicePoolBuilder
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder physicalDevices(List<LsBlkEntry> entries)
    {
        try
        {
            MsgPhysicalDevices.newBuilder()
                .addAllDevices(entries.stream().map(lsBlkEntry ->
                    MsgPhysicalDevicesOuterClass.LsBlkEntry.newBuilder()
                        .setName(lsBlkEntry.getName())
                        .setSize(lsBlkEntry.getSize())
                        .setRotational(lsBlkEntry.isRotational())
                        .setKernelName(lsBlkEntry.getKernelName())
                        .setParentName(lsBlkEntry.getParentName())
                        .setMajor(lsBlkEntry.getMajor())
                        .setMinor(lsBlkEntry.getMinor())
                        .build()).collect(Collectors.toList()))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder cryptKey(
        byte[] cryptKey,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntCryptKey.newBuilder()
                .setCryptKey(ByteString.copyFrom(cryptKey))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private void appendObjectId(UUID objUuid, String objName)
    {
        try
        {
            IntObjectId.Builder msgBuilder = IntObjectId.newBuilder();
            if (objUuid != null)
            {
                msgBuilder.setUuid(objUuid.toString());
            }
            msgBuilder
                .setName(objName)
                .build()
            .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
             handleIOException(exc);
        }
    }

    private IntStorPool buildIntStorPoolMsg(StorPool storPool)
        throws AccessDeniedException
    {
        StorPoolDefinition storPoolDfn = storPool.getDefinition(serializerCtx);
        return IntStorPool.newBuilder()
            .setStorPool(ProtoCommonSerializerBuilder.serializeStorPool(serializerCtx, storPool))
            .setStorPoolDfn(ProtoCommonSerializerBuilder.serializeStorPoolDfn(serializerCtx, storPoolDfn))
            .build();
    }

    private class NodeSerializerHelper
    {
        private IntNode buildNodeMsg(Node node, Collection<Node> relatedNodes)
            throws AccessDeniedException
        {
            return IntNode.newBuilder()
                .setUuid(node.getUuid().toString())
                .setName(node.getName().displayValue)
                .setFlags(node.getFlags().getFlagsBits(serializerCtx))
                .setType(node.getNodeType(serializerCtx).name())
                .addAllNetIfs(
                    getNetIfs(node)
                )
                .addAllNodeConns(
                    getNodeConns(node, relatedNodes)
                )
                .putAllProps(
                    node.getProps(serializerCtx).map()
                )
                .build();
        }

        private Iterable<? extends IntNetIf> getNetIfs(Node node) throws AccessDeniedException
        {
            ArrayList<IntNetIf> netIfs = new ArrayList<>();
            for (NetInterface netIf : node.streamNetInterfaces(serializerCtx).collect(toList()))
            {
                Builder builder = IntNetIf.newBuilder()
                    .setUuid(netIf.getUuid().toString())
                    .setName(netIf.getName().displayValue)
                    .setAddr(netIf.getAddress(serializerCtx).getAddress());
                if (netIf.getStltConnPort(serializerCtx) != null)
                {
                    builder.setStltConnPort(netIf.getStltConnPort(serializerCtx).value);
                }
                if (netIf.getStltConnEncryptionType(serializerCtx) != null)
                {
                    builder.setStltConnEncrType(netIf.getStltConnEncryptionType(serializerCtx).name());
                }
                netIfs.add(builder.build());
            }
            return netIfs;
        }

        private ArrayList<IntNodeConn> getNodeConns(Node node, Collection<Node> otherNodes)
            throws AccessDeniedException
        {
            ArrayList<IntNodeConn> nodeConns = new ArrayList<>();
            for (Node otherNode : otherNodes)
            {
                NodeConnection nodeConnection = node.getNodeConnection(serializerCtx, otherNode);
                String otherName;

                if (nodeConnection != null)
                {
                    if (nodeConnection.getSourceNode(serializerCtx) == node)
                    {
                        otherName = otherNode.getName().displayValue;
                    }
                    else
                    {
                        otherName = node.getName().displayValue;
                    }

                    nodeConns.add(
                        IntNodeConn.newBuilder()
                            .setOtherNodeUuid(otherNode.getUuid().toString())
                            .setOtherNodeName(otherName)
                            .setOtherNodeType(otherNode.getNodeType(serializerCtx).name())
                            .setOtherNodeFlags(otherNode.getFlags().getFlagsBits(serializerCtx))
                            .setNodeConnUuid(nodeConnection.getUuid().toString())
                            .putAllNodeConnProps(nodeConnection.getProps(serializerCtx).map())
                            .build()
                    );
                }
            }
            return nodeConns;
        }
    }

    private class CtrlSerializerHelper
    {
        private Props ctrlConfProps;

        CtrlSerializerHelper(
            final Props ctrlConfRef
        )
        {
            ctrlConfProps = ctrlConfRef;
        }

        private MsgIntApplyController buildApplyControllerMsg(long fullSyncTimeStamp, long updateId)
        {
            return MsgIntApplyController.newBuilder()
                .setCtrl(buildControllerDataMsg())
                .setFullSyncId(fullSyncTimeStamp)
                .setUpdateId(updateId)
                .build();
        }

        private IntController buildControllerDataMsg()
        {
            return IntController.newBuilder()
                .putAllProps(ctrlConfProps.map())
                .build();
        }
    }

    private class ResourceSerializerHelper
    {
        private IntRsc buildIntResource(Resource localResource)
            throws AccessDeniedException
        {
            List<Resource> otherResources = new ArrayList<>();
            Iterator<Resource> rscIterator = localResource.getDefinition().iterateResource(serializerCtx);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();
                if (!rsc.equals(localResource))
                {
                    otherResources.add(rsc);
                }
            }

            ResourceDefinition rscDfn = localResource.getDefinition();

            return IntRsc.newBuilder()
                .setLocalRsc(ProtoCommonSerializerBuilder.serializeResource(serializerCtx, localResource))
                .setRscDfn(ProtoCommonSerializerBuilder.serializeResourceDefinition(serializerCtx, rscDfn))
                .addAllOtherResources(buildOtherResources(otherResources))
                .addAllRscConnections(
                    ProtoCommonSerializerBuilder.serializeResourceConnections(
                        serializerCtx,
                        localResource.streamAbsResourceConnections(serializerCtx).collect(Collectors.toList())
                    )
                )
                .build();
        }


        private List<IntOtherRsc> buildOtherResources(List<Resource> otherResources)
            throws AccessDeniedException
        {
            List<IntOtherRsc> list = new ArrayList<>();

            for (Resource rsc : otherResources)
            {
                list.add(
                    IntOtherRsc.newBuilder()
                        .setNode(ProtoCommonSerializerBuilder.serializeNode(
                            serializerCtx,
                            rsc.getNode())
                        )
                        .setRsc(ProtoCommonSerializerBuilder.serializeResource(serializerCtx, rsc))
                        .build()
                );
            }

            return list;
        }
    }

    private class SnapshotSerializerHelper
    {
        private IntSnapshot buildSnapshotMsg(Snapshot snapshot)
            throws AccessDeniedException
        {
            SnapshotDefinition snapshotDfn = snapshot.getSnapshotDefinition();
            ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

            List<IntSnapshotOuterClass.SnapshotVlmDfn> snapshotVlmDfns = new ArrayList<>();
            for (SnapshotVolumeDefinition snapshotVolumeDefinition :
                snapshotDfn.getAllSnapshotVolumeDefinitions(serializerCtx))
            {
                snapshotVlmDfns.add(
                    IntSnapshotOuterClass.SnapshotVlmDfn.newBuilder()
                        .setSnapshotVlmDfnUuid(snapshotVolumeDefinition.getUuid().toString())
                        .setVlmNr(snapshotVolumeDefinition.getVolumeNumber().value)
                        .setVlmSize(snapshotVolumeDefinition.getVolumeSize(serializerCtx))
                        .setFlags(snapshotVolumeDefinition.getFlags().getFlagsBits(serializerCtx))
                        .build()
                );
            }

            List<IntSnapshotOuterClass.SnapshotVlm> snapshotVlms = new ArrayList<>();
            Iterator<SnapshotVolume> snapVlmIt = snapshot.iterateVolumes();
            while (snapVlmIt.hasNext())
            {
                SnapshotVolume snapshotVolume = snapVlmIt.next();
                snapshotVlms.add(
                    IntSnapshotOuterClass.SnapshotVlm.newBuilder()
                        .setSnapshotVlmUuid(snapshotVolume.getUuid().toString())
                        .setSnapshotVlmDfnUuid(snapshotDfn.getUuid().toString())
                        .setVlmNr(snapshotVolume.getVolumeNumber().value)
                        .build()
                );
            }

            return IntSnapshot.newBuilder()
                .setNodeName(snapshot.getNodeName().displayValue)
                .setRscName(rscDfn.getName().displayValue)
                .setRscDfnUuid(rscDfn.getUuid().toString())
                .setRscDfnFlags(rscDfn.getFlags().getFlagsBits(serializerCtx))
                .setRscGrp(serializeResourceGroup(serializerCtx, rscDfn.getResourceGroup()))
                .putAllRscDfnProps(rscDfn.getProps(serializerCtx).map())
                .setSnapshotUuid(snapshot.getUuid().toString())
                .setSnapshotName(snapshotDfn.getName().displayValue)
                .setSnapshotDfnUuid(snapshotDfn.getUuid().toString())
                .addAllSnapshotVlmDfns(snapshotVlmDfns)
                .setSnapshotDfnFlags(snapshotDfn.getFlags().getFlagsBits(serializerCtx))
                .putAllSnapshotDfnProps(snapshotDfn.getProps(serializerCtx).map())
                .addAllSnapshotVlms(snapshotVlms)
                .setFlags(snapshot.getFlags().getFlagsBits(serializerCtx))
                .setSuspendResource(snapshot.getSuspendResource(serializerCtx))
                .setTakeSnapshot(snapshot.getTakeSnapshot(serializerCtx))
                .setLayerObject(
                    LayerObjectSerializer.serializeLayerObject(
                        snapshot.getApiData(serializerCtx, null, null).getLayerData()
                    )
                )
                .build();
        }
    }

    public static StorPoolFreeSpace.Builder buildStorPoolFreeSpace(
        Map.Entry<StorPool, Either<SpaceInfo, ApiRcException>> entry
    )
    {
        StorPool storPool = entry.getKey();

        StorPoolFreeSpace.Builder freeSpaceBuilder = StorPoolFreeSpace.newBuilder()
            .setStorPoolUuid(storPool.getUuid().toString())
            .setStorPoolName(storPool.getName().displayValue);

        entry.getValue().consume(
            spaceInfo -> freeSpaceBuilder
                .setFreeCapacity(spaceInfo.freeCapacity)
                .setTotalCapacity(spaceInfo.totalCapacity),
            apiRcException -> freeSpaceBuilder
                // required field
                .setFreeCapacity(0L)
                // required field
                .setTotalCapacity(0L)
                .addAllErrors(serializeApiCallRc(apiRcException.getApiCallRc()))
        );

        return freeSpaceBuilder;
    }

}
