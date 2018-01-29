package com.linbit.linstor.api.protobuf.serializer;

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

import com.google.protobuf.ByteString;
import com.linbit.InvalidNameException;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnection;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.AbsCtrlStltSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.MsgDelRscOuterClass;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntDelVlmOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntPrimaryOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NetIf;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NodeConn;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;

public class ProtoCtrlStltSerializer extends AbsCtrlStltSerializer
{
    private final ResourceSerializerHelper rscSerializerHelper;
    private final NodeSerializerHelper nodeSerializerHelper;

    public ProtoCtrlStltSerializer(ErrorReporter errReporter, AccessContext serializerCtx)
    {
        super(errReporter, serializerCtx);

        rscSerializerHelper = new ResourceSerializerHelper();
        nodeSerializerHelper = new NodeSerializerHelper();
    }

    @Override
    public void writeHeader(String apiCall, int msgId, ByteArrayOutputStream baos) throws IOException
    {
        MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(apiCall)
            .setMsgId(msgId)
            .build()
            .writeDelimitedTo(baos);
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
    public void writeChangedStorPool(UUID storPoolUuid, String storPoolName, ByteArrayOutputStream baos) throws IOException
    {
        appendObjectId(storPoolUuid, storPoolName, baos);
    }

    @Override
    public void writeNodeData(
        Node node,
        Collection<Node> relatedNodes,
        long fullSyncTimestamp,
        long serializerId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException, InvalidNameException
    {
        nodeSerializerHelper
            .buildNodeDataMsg(node, relatedNodes, fullSyncTimestamp, serializerId)
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
    public void writeFullSync(
        Set<Node> nodeSet,
        Set<StorPool> storPools,
        Set<Resource> resources,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException, InvalidNameException
    {
        ArrayList<MsgIntNodeData> serializedNodes = new ArrayList<>();
        ArrayList<MsgIntStorPoolData> serializedStorPools = new ArrayList<>();
        ArrayList<MsgIntRscData> serializedRscs = new ArrayList<>();

        LinkedList<Node> nodes = new LinkedList<Node>(nodeSet);
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
            serializedRscs.add(
                rscSerializerHelper.buildResourceDataMsg(
                    rsc,
                    fullSyncTimestamp,
                    updateId
                )
            );
        }

        MsgIntFullSync.newBuilder()
            .addAllNodes(serializedNodes)
            .addAllStorPools(serializedStorPools)
            .addAllRscs(serializedRscs)
            .setFullSyncTimestamp(fullSyncTimestamp)
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
    public void writeResourceState(String nodeName, ResourceState rscState, ByteArrayOutputStream baos) throws IOException
    {
        ProtoCommonSerializer.buildResourceState(nodeName, rscState)
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeNotifyResourceDeleted(
        String nodeName,
        String resourceName,
        UUID rscUuid,
        ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgDelRscOuterClass.MsgDelRsc.newBuilder()
            .setNodeName(nodeName)
            .setRscName(resourceName)
            .setUuid(rscUuid.toString())
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

    /*
     * Helper methods
     */
    private void appendObjectId(UUID objUuid, String objName, ByteArrayOutputStream baos) throws IOException
    {
        MsgIntObjectIdOuterClass.MsgIntObjectId.Builder msgBuilder = MsgIntObjectIdOuterClass.MsgIntObjectId.newBuilder();
        if (objUuid != null)
        {
            msgBuilder.setUuid(objUuid.toString());
        }
        msgBuilder
            .setName(objName)
            .build()
            .writeDelimitedTo(baos);
    }

    private MsgIntStorPoolData buildStorPoolDataMsg(StorPool storPool, long fullSyncTimestamp, long updateId) throws AccessDeniedException
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
        return BaseProtoApiCall.fromMap(props.map());
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
                    BaseProtoApiCall.fromMap(node.getProps(serializerCtx).map())
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
            Iterator<NetInterface> iterateNetInterfaces = node.iterateNetInterfaces(serializerCtx);
            while (iterateNetInterfaces.hasNext())
            {
                NetInterface netIf = iterateNetInterfaces.next();
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
                               BaseProtoApiCall.fromMap(
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

    private class ResourceSerializerHelper
    {

        private MsgIntRscData buildResourceDataMsg(Resource localResource, long fullSyncTimestamp, long updateId) throws AccessDeniedException
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
                .addAllRscDfnProps(BaseProtoApiCall.fromMap(rscDfnProps))
                .setLocalRscUuid(localResource.getUuid().toString())
                .setLocalRscFlags(localResource.getStateFlags().getFlagsBits(serializerCtx))
                .setLocalRscNodeId(localResource.getNodeId().value)
                .addAllLocalRscProps(BaseProtoApiCall.fromMap(rscProps))
                .addAllVlmDfns(
                    buildVlmDfnMessages(localResource)
                )
                .addAllLocalVolumes(
                    buildVlmMessages(localResource)
                )
                .addAllOtherResources(
                    buildOtherResources(otherResources)
                )
                .setRscDfnTransportType(rscDfn.getTransportType(serializerCtx).name())
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build();
            return message;
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
                        .addAllVlmProps(BaseProtoApiCall.fromMap(vlmDfnProps))
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
                    .addAllVlmProps(BaseProtoApiCall.fromMap(volProps));
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
                        .addAllRscProps(BaseProtoApiCall.fromMap(rscProps))
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
                .addAllProps(BaseProtoApiCall.fromMap(nodeProps))
                .addAllNetInterfaces(buildNodeNetInterfaces(node))
                .build();
        }

        private Iterable<? extends NetInterfaceOuterClass.NetInterface> buildNodeNetInterfaces(Node node)
            throws AccessDeniedException
        {
            List<NetInterfaceOuterClass.NetInterface> protoNetIfs = new ArrayList<>();

            Iterator<NetInterface> netIfs = node.iterateNetInterfaces(serializerCtx);
            while (netIfs.hasNext())
            {
                NetInterface netIf = netIfs.next();

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
}
