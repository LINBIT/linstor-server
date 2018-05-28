package com.linbit.linstor.api.protobuf.serializer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnection;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.CtrlStltSerializerBuilderImpl;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtoStorPoolFreeSpaceUtils;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.MsgDelRscOuterClass.MsgDelRsc;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.javainternal.EventInProgressSnapshotOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntApplyRscSuccessOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntControllerDataOuterClass.MsgIntControllerData;
import com.linbit.linstor.proto.javainternal.MsgIntCryptKeyOuterClass.MsgIntCryptKey;
import com.linbit.linstor.proto.javainternal.MsgIntDelRscOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntDelVlmOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NetIf;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NodeConn;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDeletedDataOuterClass.MsgIntNodeDeletedData;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.proto.javainternal.MsgIntPrimaryOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.RscConnectionData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDeletedDataOuterClass.MsgIntRscDeletedData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDeletedDataOuterClass.MsgIntStorPoolDeletedData;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.utils.Base64;

import static java.util.stream.Collectors.toList;

@Singleton
public class ProtoCtrlStltSerializer extends ProtoCommonSerializer
    implements CtrlStltSerializer, CtrlStltSerializerBuilderImpl.CtrlStltSerializationWriter
{
    private final CtrlSerializerHelper ctrlSerializerHelper;
    private final ResourceSerializerHelper rscSerializerHelper;
    private final NodeSerializerHelper nodeSerializerHelper;
    private final CtrlSecurityObjects secObjs;

    @Inject
    public ProtoCtrlStltSerializer(
        ErrorReporter errReporter,
        @ApiContext AccessContext serializerCtx,
        CtrlSecurityObjects secObjsRef,
        @Named(ControllerCoreModule.SATELLITE_PROPS) Props ctrlConfRef)
    {
        super(errReporter, serializerCtx);
        secObjs = secObjsRef;

        ctrlSerializerHelper = new CtrlSerializerHelper(ctrlConfRef);
        rscSerializerHelper = new ResourceSerializerHelper();
        nodeSerializerHelper = new NodeSerializerHelper();
    }

    @Override
    public CtrlStltSerializerBuilder builder()
    {
        return builder(null);
    }

    @Override
    public CtrlStltSerializerBuilder builder(String apiCall)
    {
        return builder(apiCall, 1);
    }

    @Override
    public CtrlStltSerializerBuilder builder(String apiCall, Integer msgId)
    {
        return new CtrlStltSerializerBuilderImpl(errorReporter, this, apiCall, msgId);
    }

    /*
     * Controller -> Satellite
     */
    @Override
    public void writeAuthMessage(
        UUID nodeUuid,
        String nodeName,
        byte[] sharedSecret,
        UUID nodeDisklessStorPoolDfnUuid,
        UUID nodeDisklessStorPoolUuid,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgIntAuthOuterClass.MsgIntAuth.newBuilder()
            .setNodeUuid(nodeUuid.toString())
            .setNodeName(nodeName)
            .setSharedSecret(ByteString.copyFrom(sharedSecret))
            .setNodeDisklessStorPoolDfnUuid(nodeDisklessStorPoolDfnUuid.toString())
            .setNodeDisklessStorPoolUuid(nodeDisklessStorPoolUuid.toString())
            .build()
            .writeDelimitedTo(baos);
    }

    // no fullSync- or update-id needed
    @Override
    public void writeChangedNode(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos) throws IOException
    {
        appendObjectId(nodeUuid, nodeName, baos);
    }

    // no fullSync- or update-id needed
    @Override
    public void writeChangedResource(UUID rscUuid, String rscName, ByteArrayOutputStream baos) throws IOException
    {
        appendObjectId(rscUuid, rscName, baos);
    }

    // no fullSync- or update-id needed
    @Override
    public void writeChangedStorPool(UUID storPoolUuid, String storPoolName, ByteArrayOutputStream baos)
        throws IOException
    {
        appendObjectId(storPoolUuid, storPoolName, baos);
    }

    @Override
    public void writeControllerData(
        long fullSyncTimestamp,
        long serializerid,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        ctrlSerializerHelper
            .buildControllerDataMsg(fullSyncTimestamp, serializerid)
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeNodeData(
        Node node,
        Collection<Node> relatedNodes,
        long fullSyncTimestamp,
        long serializerId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException
    {
        nodeSerializerHelper
            .buildNodeDataMsg(node, relatedNodes, fullSyncTimestamp, serializerId)
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeDeletedNodeData(
        String nodeNameStr,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgIntNodeDeletedData.newBuilder()
            .setNodeName(nodeNameStr)
            .setFullSyncId(fullSyncTimestamp)
            .setUpdateId(updateId)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeResourceData(
        Resource localResource,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException
    {
        rscSerializerHelper
            .buildResourceDataMsg(localResource, fullSyncTimestamp, updateId)
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeDeletedResourceData(
        String rscNameStr,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgIntRscDeletedData.newBuilder()
            .setRscName(rscNameStr)
            .setFullSyncId(fullSyncTimestamp)
            .setUpdateId(updateId)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeStorPoolData(
        StorPool storPool,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException
    {
        buildStorPoolDataMsg(storPool, fullSyncTimestamp, updateId)
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeDeletedStorPoolData(
        String storPoolNameStr,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgIntStorPoolDeletedData.newBuilder()
            .setStorPoolName(storPoolNameStr)
            .setFullSyncId(fullSyncTimestamp)
            .setUpdateId(updateId)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeFullSync(
        Set<Node> nodeSet,
        Set<StorPool> storPools,
        Set<Resource> resources,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException
    {
        ArrayList<MsgIntNodeData> serializedNodes = new ArrayList<>();
        ArrayList<MsgIntStorPoolData> serializedStorPools = new ArrayList<>();
        ArrayList<MsgIntRscData> serializedRscs = new ArrayList<>();

        MsgIntControllerData serializedController = ctrlSerializerHelper.buildControllerDataMsg(
            fullSyncTimestamp,
            updateId
        );

        LinkedList<Node> nodes = new LinkedList<>(nodeSet);

        while (!nodes.isEmpty())
        {
            Node node = nodes.removeFirst();
            serializedNodes.add(
                nodeSerializerHelper.buildNodeDataMsg(
                    node,
                    nodes,
                    fullSyncTimestamp,
                    updateId
                )
            );
        }
        for (StorPool storPool : storPools)
        {
            serializedStorPools.add(
                buildStorPoolDataMsg(
                    storPool,
                    fullSyncTimestamp,
                    updateId
                )
            );
        }
        for (Resource rsc : resources)
        {
            if (rsc.iterateVolumes().hasNext())
            {
                serializedRscs.add(
                    rscSerializerHelper.buildResourceDataMsg(
                        rsc,
                        fullSyncTimestamp,
                        updateId
                    )
                );
            }
        }

        String encodedMasterKey = "";
        byte[] cryptKey = secObjs.getCryptKey();
        if (cryptKey != null)
        {
            encodedMasterKey = Base64.encode(cryptKey);
        }
        MsgIntFullSync.newBuilder()
            .addAllNodes(serializedNodes)
            .addAllStorPools(serializedStorPools)
            .addAllRscs(serializedRscs)
            .setFullSyncTimestamp(fullSyncTimestamp)
            .setMasterKey(encodedMasterKey)
            .setCtrlData(serializedController)
            .build()
            .writeDelimitedTo(baos);
    }

    /*
     * Satellite -> Controller
     */
    @Override
    public void writePrimaryRequest(String rscName, String rscUuid, ByteArrayOutputStream baos)
        throws IOException
    {
        MsgIntPrimaryOuterClass.MsgIntPrimary.newBuilder()
            .setRscName(rscName)
            .setRscUuid(rscUuid)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeNotifyResourceApplied(
        Resource resource,
        Map<StorPool, Long> freeSpaceMap,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException
    {
        List<MsgIntApplyRscSuccessOuterClass.VlmData> vlmDatas = new ArrayList<>();
        for (Volume vlm : resource.streamVolumes().collect(toList()))
        {
            vlmDatas.add(
                MsgIntApplyRscSuccessOuterClass.VlmData.newBuilder()
                    .setVlmNr(vlm.getVolumeDefinition().getVolumeNumber().value)
                    .setBlockDevicePath(vlm.getBlockDevicePath(serializerCtx))
                    .setMetaDisk(vlm.getMetaDiskPath(serializerCtx))
                    .build()
            );
        }

        MsgIntApplyRscSuccessOuterClass.MsgIntApplyRscSuccess.newBuilder()
            .setRscId(
                MsgIntObjectId.newBuilder()
                    .setUuid(resource.getUuid().toString())
                    .setName(resource.getDefinition().getName().displayValue)
                    .build()
            )
            .addAllFreeSpace(
                ProtoStorPoolFreeSpaceUtils.getAllStorPoolFreeSpaces(freeSpaceMap)
            )
            .addAllVlmData(vlmDatas)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeNotifyResourceDeleted(
        String nodeName,
        String resourceName,
        UUID rscUuid,
        Map<StorPool, Long> freeSpaceMap,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgIntDelRscOuterClass.MsgIntDelRsc.newBuilder()
            .setDeletedRsc(
                MsgDelRsc.newBuilder()
                    .setNodeName(nodeName)
                    .setRscName(resourceName)
                    .setUuid(rscUuid.toString())
                    .build()
            )
            .addAllFreeSpace(
                ProtoStorPoolFreeSpaceUtils.getAllStorPoolFreeSpaces(freeSpaceMap)
            )
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeNotifyVolumeDeleted(
        String nodeName,
        String resourceName,
        int volumeNr,
        UUID vlmUuid,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgIntDelVlmOuterClass.MsgIntDelVlm.newBuilder()
            .setNodeName(nodeName)
            .setRscName(resourceName)
            .setVlmNr(volumeNr)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeRequestNodeUpdate(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos) throws IOException
    {
        appendObjectId(nodeUuid, nodeName, baos);
    }

    @Override
    public void writeRequestResourceDfnUpdate(UUID rscDfnUuid, String rscName, ByteArrayOutputStream baos)
        throws IOException
    {
        appendObjectId(rscDfnUuid, rscName, baos);
    }

    @Override
    public void writeRequestResourceUpdate(UUID rscUuid, String nodeName, String rscName, ByteArrayOutputStream baos)
        throws IOException
    {
        appendObjectId(null, nodeName, baos);
        appendObjectId(rscUuid, rscName, baos);
    }

    @Override
    public void writeRequestStorPoolUpdate(UUID storPoolUuid, String storPoolName, ByteArrayOutputStream baos)
        throws IOException
    {
        appendObjectId(storPoolUuid, storPoolName, baos);
    }

    @Override
    public void writeCryptKey(
        byte[] cryptKey,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgIntCryptKey.newBuilder()
            .setCryptKey(ByteString.copyFrom(cryptKey))
            .setFullSyncId(fullSyncTimestamp)
            .setUpdateId(updateId)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeInProgressSnapshotEvent(
        SnapshotState snapshotState, ByteArrayOutputStream baos
    )
        throws IOException
    {
        EventInProgressSnapshotOuterClass.EventInProgressSnapshot.newBuilder()
            .setSuspended(snapshotState.isSuspended())
            .setSnapshotTaken(snapshotState.isSnapshotTaken())
            .build()
            .writeDelimitedTo(baos);
    }

    /*
     * Helper methods
     */
    private void appendObjectId(UUID objUuid, String objName, ByteArrayOutputStream baos) throws IOException
    {
        MsgIntObjectId.Builder msgBuilder = MsgIntObjectId.newBuilder();
        if (objUuid != null)
        {
            msgBuilder.setUuid(objUuid.toString());
        }
        msgBuilder
            .setName(objName)
            .build()
            .writeDelimitedTo(baos);
    }

    private MsgIntStorPoolData buildStorPoolDataMsg(StorPool storPool, long fullSyncTimestamp, long updateId)
        throws AccessDeniedException
    {
        StorPoolDefinition storPoolDfn = storPool.getDefinition(serializerCtx);
        MsgIntStorPoolData message = MsgIntStorPoolData.newBuilder()
            .setStorPoolUuid(storPool.getUuid().toString())
            .setNodeUuid(storPool.getNode().getUuid().toString())
            .setStorPoolDfnUuid(storPoolDfn.getUuid().toString())
            .setStorPoolName(storPool.getName().displayValue)
            .setDriver(storPool.getDriverName())
            .addAllStorPoolProps(asLinStorList(storPool.getProps(serializerCtx)))
            .addAllStorPoolDfnProps(asLinStorList(storPoolDfn.getProps(serializerCtx)))
            .setFullSyncId(fullSyncTimestamp)
            .setUpdateId(updateId)
            .build();
        return message;
    }

    private List<LinStorMapEntry> asLinStorList(Props props)
    {
        return ProtoMapUtils.fromMap(props.map());
    }

    private class NodeSerializerHelper
    {
        private MsgIntNodeData buildNodeDataMsg(
            Node node,
            Collection<Node> relatedNodes,
            long fullSyncTimestamp,
            long updateId
        )
            throws AccessDeniedException
        {
            return MsgIntNodeData.newBuilder()
                .setNodeUuid(node.getUuid().toString())
                .setNodeName(node.getName().displayValue)
                .setNodeFlags(node.getFlags().getFlagsBits(serializerCtx))
                .setNodeType(node.getNodeType(serializerCtx).name())
                .addAllNodeNetIfs(
                    getNetIfs(node)
                )
                .addAllNodeConns(
                    getNodeConns(node, relatedNodes)
                )
                .addAllNodeProps(
                    ProtoMapUtils.fromMap(node.getProps(serializerCtx).map())
                )
                .setNodeDisklessStorPoolUuid(
                    node.getDisklessStorPool(serializerCtx).getUuid().toString()
                )
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build();
        }

        private Iterable<? extends NetIf> getNetIfs(Node node) throws AccessDeniedException
        {
            ArrayList<NetIf> netIfs = new ArrayList<>();
            for (NetInterface netIf : node.streamNetInterfaces(serializerCtx).collect(toList()))
            {
                netIfs.add(
                    NetIf.newBuilder()
                        .setNetIfUuid(netIf.getUuid().toString())
                        .setNetIfName(netIf.getName().displayValue)
                        .setNetIfAddr(netIf.getAddress(serializerCtx).getAddress())
                        .build()
                );
            }
            return netIfs;
        }

        private ArrayList<NodeConn> getNodeConns(Node node, Collection<Node> otherNodes)
            throws AccessDeniedException
        {
            ArrayList<NodeConn> nodeConns = new ArrayList<>();
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
                       NodeConn.newBuilder()
                           .setOtherNodeUuid(otherNode.getUuid().toString())
                           .setOtherNodeName(otherName)
                           .setOtherNodeType(otherNode.getNodeType(serializerCtx).name())
                           .setOtherNodeFlags(otherNode.getFlags().getFlagsBits(serializerCtx))
                           .setNodeConnUuid(nodeConnection.getUuid().toString())
                           .addAllNodeConnProps(
                               ProtoMapUtils.fromMap(
                                   nodeConnection.getProps(serializerCtx).map()
                               )
                           )
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

        private MsgIntControllerData buildControllerDataMsg(
            long fullSyncTimestamp,
            long updateId)
        {
            return MsgIntControllerData.newBuilder()
                .addAllControllerProps(ProtoMapUtils.fromMap(ctrlConfProps.map()))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build();
        }
    }

    private class ResourceSerializerHelper
    {
        private MsgIntRscData buildResourceDataMsg(Resource localResource, long fullSyncTimestamp, long updateId)
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
            String rscName = rscDfn.getName().displayValue;
            Map<String, String> rscDfnProps = rscDfn.getProps(serializerCtx).map();
            Map<String, String> rscProps = localResource.getProps(serializerCtx).map();

            MsgIntRscData message = MsgIntRscData.newBuilder()
                .setRscName(rscName)
                .setRscDfnUuid(rscDfn.getUuid().toString())
                .setRscDfnPort(rscDfn.getPort(serializerCtx).value)
                .setRscDfnFlags(rscDfn.getFlags().getFlagsBits(serializerCtx))
                .setRscDfnSecret(rscDfn.getSecret(serializerCtx))
                .addAllRscDfnProps(ProtoMapUtils.fromMap(rscDfnProps))
                .setLocalRscUuid(localResource.getUuid().toString())
                .setLocalRscFlags(localResource.getStateFlags().getFlagsBits(serializerCtx))
                .setLocalRscNodeId(localResource.getNodeId().value)
                .addAllLocalRscProps(ProtoMapUtils.fromMap(rscProps))
                .addAllVlmDfns(
                    buildVlmDfnMessages(localResource)
                )
                .addAllLocalVolumes(
                    buildVlmMessages(localResource)
                )
                .addAllOtherResources(
                    buildOtherResources(otherResources)
                )
                .addAllRscConnections(buildRscConnections(localResource))
                .setRscDfnTransportType(rscDfn.getTransportType(serializerCtx).name())
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .addAllInProgressSnapshots(
                    buildInProgressSnapshots(localResource)
                )
                .build();
            return message;
        }

        private Iterable<? extends RscConnectionData> buildRscConnections(Resource localResource)
            throws AccessDeniedException
        {
            List<RscConnectionData> list = new ArrayList<>();

            for (ResourceConnection conn : localResource.streamResourceConnections(serializerCtx)
                .collect(Collectors.toList()))
            {
                list.add(RscConnectionData.newBuilder()
                    .setUuid(conn.getUuid().toString())
                    .setNode1(conn.getSourceResource(serializerCtx).getAssignedNode().getName().getDisplayName())
                    .setNode2(conn.getTargetResource(serializerCtx).getAssignedNode().getName().getDisplayName())
                    .addAllProps(ProtoMapUtils.fromMap(conn.getProps(serializerCtx).map()))
                    .build()
                );
            }
            return list;
        }

        private Iterable<? extends VlmDfn> buildVlmDfnMessages(Resource localResource)
            throws AccessDeniedException
        {
            List<VlmDfn> list = new ArrayList<>();

            Iterator<Volume> localVolIterator = localResource.iterateVolumes();
            while (localVolIterator.hasNext())
            {
                Volume vol = localVolIterator.next();
                VolumeDefinition vlmDfn = vol.getVolumeDefinition();

                Map<String, String> vlmDfnProps = vlmDfn.getProps(serializerCtx).map();
                list.add(
                    VlmDfn.newBuilder()
                        .setVlmDfnUuid(vlmDfn.getUuid().toString())
                        .setVlmNr(vlmDfn.getVolumeNumber().value)
                        .setVlmSize(vlmDfn.getVolumeSize(serializerCtx))
                        .setVlmMinor(vlmDfn.getMinorNr(serializerCtx).value)
                        .addAllVlmFlags(
                            FlagsHelper.toStringList(
                                VolumeDefinition.VlmDfnFlags.class,
                                vlmDfn.getFlags().getFlagsBits(serializerCtx)
                            )
                        )
                        .addAllVlmProps(ProtoMapUtils.fromMap(vlmDfnProps))
                        .build()
                );
            }

            return list;
        }

        private List<Vlm> buildVlmMessages(Resource rsc)
            throws AccessDeniedException
        {
            List<Vlm> vlmList = new ArrayList<>();

            Iterator<Volume> volIterator = rsc.iterateVolumes();
            while (volIterator.hasNext())
            {
                Volume vol = volIterator.next();
                Map<String, String> volProps = vol.getProps(serializerCtx).map();
                StorPool vlmStorPool = vol.getStorPool(serializerCtx);
                Vlm.Builder builder = Vlm.newBuilder()
                    .setVlmDfnUuid(vol.getVolumeDefinition().getUuid().toString())
                    .setVlmUuid(vol.getUuid().toString())
                    .setVlmNr(vol.getVolumeDefinition().getVolumeNumber().value)
                    .setVlmMinorNr(vol.getVolumeDefinition().getMinorNr(serializerCtx).value)
                    .addAllVlmFlags(Volume.VlmFlags.toStringList(vol.getFlags().getFlagsBits(serializerCtx)))
                    .setStorPoolUuid(vlmStorPool.getUuid().toString())
                    .setStorPoolName(vlmStorPool.getName().displayValue)
                    .setStorPoolDriverName(vlmStorPool.getDriverName())
                    .setStorPoolDfnUuid(vlmStorPool.getDefinition(serializerCtx).getUuid().toString())
                    .addAllStorPoolDfnProps(
                        ProtoMapUtils.fromMap(
                            vlmStorPool.getDefinition(serializerCtx).getProps(serializerCtx).map())
                        )
                    .addAllStorPoolProps(
                        ProtoMapUtils.fromMap(
                            vlmStorPool.getProps(serializerCtx).map())
                        )
                    .addAllVlmProps(ProtoMapUtils.fromMap(volProps));
                String blockDev = vol.getBlockDevicePath(serializerCtx);
                if (blockDev != null)
                {
                    builder.setBlockDevice(blockDev);
                }
                String metaDisk = vol.getMetaDiskPath(serializerCtx);
                if (metaDisk != null)
                {
                    builder.setMetaDisk(metaDisk);
                }
                vlmList.add(builder.build());
            }
            return vlmList;
        }

        private List<MsgIntOtherRscData> buildOtherResources(List<Resource> otherResources)
            throws AccessDeniedException
        {
            List<MsgIntOtherRscData> list = new ArrayList<>();

            for (Resource rsc : otherResources)
            {
                Node node = rsc.getAssignedNode();
                Map<String, String> rscProps = rsc.getProps(serializerCtx).map();
                list.add(
                    MsgIntOtherRscData.newBuilder()
                        .setNode(buildOtherNode(node))
                        .setNodeFlags(node.getFlags().getFlagsBits(serializerCtx))
                        .setRscUuid(rsc.getUuid().toString())
                        .setRscNodeId(rsc.getNodeId().value)
                        .setRscFlags(rsc.getStateFlags().getFlagsBits(serializerCtx))
                        .addAllRscProps(ProtoMapUtils.fromMap(rscProps))
                        .addAllLocalVlms(
                            buildVlmMessages(rsc)
                        )
                        .build()
                );
            }

            return list;
        }

        private NodeOuterClass.Node buildOtherNode(Node node) throws AccessDeniedException
        {
            Map<String, String> nodeProps = node.getProps(serializerCtx).map();
            return NodeOuterClass.Node.newBuilder()
                .setUuid(node.getUuid().toString())
                .setName(node.getName().displayValue)
                .setType(node.getNodeType(serializerCtx).name())
                .setDisklessStorPoolUuid(node.getDisklessStorPool(serializerCtx).getUuid().toString())
                .addAllProps(ProtoMapUtils.fromMap(nodeProps))
                .addAllNetInterfaces(buildNodeNetInterfaces(node))
                .build();
        }

        private Iterable<? extends NetInterfaceOuterClass.NetInterface> buildNodeNetInterfaces(Node node)
            throws AccessDeniedException
        {
            List<NetInterfaceOuterClass.NetInterface> protoNetIfs = new ArrayList<>();

            for (NetInterface netIf : node.streamNetInterfaces(serializerCtx).collect(toList()))
            {
                protoNetIfs.add(
                    NetInterfaceOuterClass.NetInterface.newBuilder()
                        .setUuid(netIf.getUuid().toString())
                        .setName(netIf.getName().displayValue)
                        .setAddress(netIf.getAddress(serializerCtx).getAddress())
                        .build()
                );
            }

            return protoNetIfs;
        }
    }

    private Iterable<? extends MsgIntRscDataOuterClass.IntSnapshot> buildInProgressSnapshots(Resource localResource)
    {
        List<MsgIntRscDataOuterClass.IntSnapshot> snapshots = new ArrayList<>();

        for (Snapshot snapshot : localResource.getInProgressSnapshots())
        {
            snapshots.add(
                MsgIntRscDataOuterClass.IntSnapshot.newBuilder()
                    .setSnapshotUuid(snapshot.getUuid().toString())
                    .setSnapshotName(snapshot.getSnapshotDefinition().getName().displayValue)
                    .setSnapshotDefinitionUuid(snapshot.getSnapshotDefinition().getUuid().toString())
                    .setSuspendResource(snapshot.getSuspendResource())
                    .setTakeSnapshot(snapshot.getTakeSnapshot())
                    .build()
            );
        }

        return snapshots;
    }
}
