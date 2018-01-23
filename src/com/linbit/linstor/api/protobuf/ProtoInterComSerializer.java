package com.linbit.linstor.api.protobuf;

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
import com.linbit.ImplementationError;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnection;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.interfaces.serializer.InterComBuilder;
import com.linbit.linstor.api.interfaces.serializer.InterComSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.MsgDelRscOuterClass;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgLstNodeOuterClass;
import com.linbit.linstor.proto.MsgLstRscDfnOuterClass;
import com.linbit.linstor.proto.MsgLstRscOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolDfnOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolOuterClass;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.RscStateOuterClass;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.VlmStateOuterClass;
import com.linbit.linstor.proto.apidata.NodeApiData;
import com.linbit.linstor.proto.apidata.RscApiData;
import com.linbit.linstor.proto.apidata.RscDfnApiData;
import com.linbit.linstor.proto.apidata.StorPoolApiData;
import com.linbit.linstor.proto.apidata.StorPoolDfnApiData;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntDelVlmOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NetIf;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NodeConn;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId.Builder;
import com.linbit.linstor.proto.javainternal.MsgIntPrimaryOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;

/**
 *
 * @author rpeinthor
 */
public class ProtoInterComSerializer implements InterComSerializer
{
    private final ErrorReporter errReporter;
    private final AccessContext serializerCtx;
    private final ResourceSerializerHelper rscSerializerHelper;
    private final NodeSerializerHelper nodeSerializerHelper;

    public ProtoInterComSerializer(
        final ErrorReporter errReporterRef,
        final AccessContext serializerCtxRef
    )
    {
        errReporter = errReporterRef;
        serializerCtx = serializerCtxRef;
        rscSerializerHelper = new ResourceSerializerHelper();
        nodeSerializerHelper = new NodeSerializerHelper();
    }

    @Override
    public InterComBuilder builder(String apiCall, int msgId)
    {
        return new ProtoInterComBuilder(apiCall, msgId);
    }

    class ProtoInterComBuilder implements InterComBuilder
    {
        private final ByteArrayOutputStream baos;
        private boolean exceptionOccured = false;

        public ProtoInterComBuilder(final String apiCall, final int msgId)
        {
            baos = new ByteArrayOutputStream();

            try
            {
                MsgHeaderOuterClass.MsgHeader.newBuilder()
                    .setApiCall(apiCall)
                    .setMsgId(msgId)
                    .build()
                    .writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }
        }

        @Override
        public byte[] build()
        {
            byte[] ret;
            if (exceptionOccured)
            {
                ret = new byte[0]; // do not send corrupted data
            }
            else
            {
                ret = baos.toByteArray();
            }
            return ret;
        }

        @Override
        public InterComBuilder primaryRequest(String rscName, String rscUuid)
        {
            try
            {
                MsgIntPrimaryOuterClass.MsgIntPrimary.Builder msgReqPrimary = MsgIntPrimaryOuterClass.MsgIntPrimary.newBuilder();
                msgReqPrimary.setRscName(rscName);
                msgReqPrimary.setRscUuid(rscUuid);

                msgReqPrimary.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public InterComBuilder nodeList(List<Node.NodeApi> nodes)
        {
            MsgLstNodeOuterClass.MsgLstNode.Builder msgListNodeBuilder = MsgLstNodeOuterClass.MsgLstNode.newBuilder();

            try
            {
                for (Node.NodeApi apiNode: nodes)
                {
                    msgListNodeBuilder.addNodes(NodeApiData.toNodeProto(apiNode));
                }

                msgListNodeBuilder.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public InterComBuilder storPoolDfnList(List<StorPoolDefinition.StorPoolDfnApi> storpooldfns)
        {
            MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.Builder msgListStorPoolDfnsBuilder =
                    MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.newBuilder();

            for (StorPoolDefinition.StorPoolDfnApi apiDfn: storpooldfns)
            {
                msgListStorPoolDfnsBuilder.addStorPoolDfns(StorPoolDfnApiData.fromStorPoolDfnApi(apiDfn));
            }

            try
            {
                msgListStorPoolDfnsBuilder.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public InterComBuilder storPoolList(List<StorPool.StorPoolApi> storpools)
        {
            MsgLstStorPoolOuterClass.MsgLstStorPool.Builder msgListBuilder =
                    MsgLstStorPoolOuterClass.MsgLstStorPool.newBuilder();
            for (StorPool.StorPoolApi apiStorPool: storpools)
            {
                msgListBuilder.addStorPools(StorPoolApiData.toStorPoolProto(apiStorPool));
            }

            try
            {
                msgListBuilder.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }

            return this;
        }

        @Override
        public InterComBuilder resourceDfnList(List<ResourceDefinition.RscDfnApi> rscDfns)
        {
            MsgLstRscDfnOuterClass.MsgLstRscDfn.Builder msgListRscDfnsBuilder = MsgLstRscDfnOuterClass.MsgLstRscDfn.newBuilder();

            for (ResourceDefinition.RscDfnApi apiRscDfn: rscDfns)
            {
                msgListRscDfnsBuilder.addRscDfns(RscDfnApiData.fromRscDfnApi(apiRscDfn));
            }

            try
            {
                msgListRscDfnsBuilder.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public InterComBuilder resourceList(final List<Resource.RscApi> rscs, final Collection<ResourceState> rscStates)
        {
            MsgLstRscOuterClass.MsgLstRsc.Builder msgListRscsBuilder = MsgLstRscOuterClass.MsgLstRsc.newBuilder();

            for (Resource.RscApi apiRsc: rscs)
            {
                msgListRscsBuilder.addResources(RscApiData.toRscProto(apiRsc));
            }

            for (ResourceState rscState : rscStates)
            {
                msgListRscsBuilder.addResourceStates(buildResourceState(rscState.getNodeName(), rscState));
            }

            try
            {
                msgListRscsBuilder.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }

            return this;
        }

        private RscStateOuterClass.RscState buildResourceState(final String nodeName, final ResourceState rscState) {
            RscStateOuterClass.RscState.Builder rscStateBuilder = RscStateOuterClass.RscState.newBuilder();

            rscStateBuilder
                .setRscName(rscState.getRscName())
                .setNodeName(nodeName)
                .setIsPresent(rscState.isPresent())
                .setRequiresAdjust(rscState.requiresAdjust())
                .setIsPrimary(rscState.isPrimary());

            // volumes
            for (VolumeState vlmState : rscState.getVolumes()) {
                VlmStateOuterClass.VlmState.Builder vlmStateBuilder = VlmStateOuterClass.VlmState.newBuilder();

                vlmStateBuilder
                    .setVlmNr(vlmState.getVlmNr().value)
                    .setVlmMinorNr(vlmState.getMinorNr().value)
                    .setIsPresent(vlmState.isPresent())
                    .setHasDisk(vlmState.hasDisk())
                    .setHasMetaData(vlmState.hasMetaData())
                    .setCheckMetaData(vlmState.isCheckMetaData())
                    .setDiskFailed(vlmState.isDiskFailed())
                    .setNetSize(vlmState.getNetSize())
                    .setGrossSize(vlmState.getGrossSize());

                rscStateBuilder.addVlmStates(vlmStateBuilder);
            }

            return rscStateBuilder.build();
        }

        @Override
        public InterComBuilder resourceState(final String nodeName, final ResourceState rscState) {
            RscStateOuterClass.RscState protoRscState = buildResourceState(nodeName, rscState);

            try
            {
                protoRscState.writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }

            return this;
        }

        @Override
        public InterComBuilder notifyResourceDeleted(String nodeName, String resourceName, String rscUuid)
        {
            MsgDelRscOuterClass.MsgDelRsc.Builder msgDelRscBld = MsgDelRscOuterClass.MsgDelRsc.newBuilder();
            msgDelRscBld.setNodeName(nodeName);
            msgDelRscBld.setRscName(resourceName);
            msgDelRscBld.setUuid(rscUuid);

            try
            {
                msgDelRscBld.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }

            return this;
        }

        @Override
        public InterComBuilder notifyVolumeDeleted(String nodeName, String resourceName, int volumeNr)
        {
            MsgIntDelVlmOuterClass.MsgIntDelVlm.Builder msgDelVlmBld = MsgIntDelVlmOuterClass.MsgIntDelVlm.newBuilder();
            msgDelVlmBld.setNodeName(nodeName);
            msgDelVlmBld.setRscName(resourceName);
            msgDelVlmBld.setVlmNr(volumeNr);

            try
            {
                msgDelVlmBld.build().writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }

            return this;
        }

        @Override
        public InterComBuilder authMessage(UUID nodeUuid, String nodeName, byte[] sharedSecret)
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
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }

            return this;
        }

        @Override
        public InterComBuilder requestNodeUpdate(UUID nodeUuid, String nodeName)
        {
            appendObjectId(nodeUuid, nodeName);
            return this;
        }

        @Override
        public InterComBuilder requestResourceDfnUpdate(UUID rscDfnUuid, String rscName)
        {
            appendObjectId(rscDfnUuid, rscName);
            return this;
        }

        @Override
        public InterComBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName)
        {
            appendObjectId(null, nodeName);
            appendObjectId(rscUuid, rscName);
            return this;
        }

        @Override
        public InterComBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName)
        {
            appendObjectId(storPoolUuid, storPoolName);
            return this;
        }

        @Override
        public InterComBuilder changedNode(UUID nodeUuid, String nodeName)
        {
            appendObjectId(nodeUuid, nodeName);
            return this;
        }

        @Override
        public InterComBuilder changedResource(UUID rscUuid, String rscName)
        {
            appendObjectId(rscUuid, rscName);
            return this;
        }

        @Override
        public InterComBuilder changedStorPool(UUID storPoolUuid, String storPoolName)
        {
            appendObjectId(storPoolUuid, storPoolName);
            return this;
        }

        @Override
        public InterComBuilder nodeData(Node node, Collection<Node> relatedNodes)
        {
            try
            {
                nodeSerializerHelper
                    .buildNodeDataMsg(node, relatedNodes)
                    .writeDelimitedTo(baos);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                errReporter.reportError(
                    new ImplementationError(
                        "ProtoInterComSerializer has not enough privileges to seriailze node",
                        accDeniedExc
                    )
                );
                exceptionOccured = true;
            }
            catch (IOException ioExc)
            {
                errReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public InterComBuilder resourceData(Resource localResource)
        {
            try
            {
                rscSerializerHelper
                    .buildResourceDataMsg(localResource)
                    .writeDelimitedTo(baos);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                errReporter.reportError(
                    new ImplementationError(
                        "ProtoInterComSerializer has not enough privileges to seriailze resource",
                        accDeniedExc
                    )
                );
                exceptionOccured = true;
            }
            catch (IOException ioExc)
            {
                errReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public InterComBuilder storPoolData(StorPool storPool)
        {
            try
            {
                buildStorPoolDataMsg(storPool)
                    .writeDelimitedTo(baos);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                errReporter.reportError(
                    new ImplementationError(
                        "ProtoInterComSerializer has not enough privileges to seriailze storage pool",
                        accDeniedExc
                    )
                );
                exceptionOccured = true;
            }
            catch (IOException ioExc)
            {
                errReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public InterComBuilder fullSync(
            Set<Node> nodeSet,
            Set<StorPool> storPools,
            Set<Resource> resources
        )
        {
            try
            {
                ArrayList<MsgIntNodeData> serializedNodes = new ArrayList<>();
                ArrayList<MsgIntStorPoolData> serializedStorPools = new ArrayList<>();
                ArrayList<MsgIntRscData> serializedRscs = new ArrayList<>();

                LinkedList<Node> nodes = new LinkedList<Node>(nodeSet);
                while (!nodes.isEmpty())
                {
                    Node node = nodes.removeFirst();
                    serializedNodes.add(nodeSerializerHelper.buildNodeDataMsg(node, nodes));
                }
                for (StorPool storPool : storPools)
                {
                    serializedStorPools.add(buildStorPoolDataMsg(storPool));
                }
                for (Resource rsc : resources)
                {
                    serializedRscs.add(rscSerializerHelper.buildResourceDataMsg(rsc));
                }

                MsgIntFullSync.newBuilder()
                    .addAllNodes(serializedNodes)
                    .addAllStorPools(serializedStorPools)
                    .addAllRscs(serializedRscs)
                    .build()
                    .writeDelimitedTo(baos);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                errReporter.reportError(
                    new ImplementationError(
                        "ProtoInterComSerializer has not enough privileges to seriailze full sync",
                        accDeniedExc
                    )
                );
                exceptionOccured = true;
            }
            catch (IOException ioExc)
            {
                errReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        private void appendObjectId(UUID objUuid, String objName)
        {
            try
            {
                Builder msgBuilder = MsgIntObjectIdOuterClass.MsgIntObjectId.newBuilder();
                if (objUuid != null)
                {
                    msgBuilder.setUuid(objUuid.toString());
                }
                msgBuilder
                    .setName(objName)
                    .build()
                    .writeDelimitedTo(baos);
            }
            catch (IOException ex)
            {
                errReporter.reportError(ex);
                exceptionOccured = true;
            }
        }

        private MsgIntStorPoolData buildStorPoolDataMsg(StorPool storPool) throws AccessDeniedException
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
                .build();
            return message;
        }


        private List<LinStorMapEntry> asLinStorList(Props props)
        {
            return BaseProtoApiCall.fromMap(props.map());
        }
    }

    private class NodeSerializerHelper
    {
        private MsgIntNodeData buildNodeDataMsg(Node node, Collection<Node> relatedNodes) throws AccessDeniedException
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

        private MsgIntRscData buildResourceDataMsg(Resource localResource) throws AccessDeniedException
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
