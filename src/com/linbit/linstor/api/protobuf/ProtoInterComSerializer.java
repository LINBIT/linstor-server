package com.linbit.linstor.api.protobuf;

import com.google.protobuf.ByteString;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.api.interfaces.serializer.InterComBuilder;
import com.linbit.linstor.api.interfaces.serializer.InterComSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgDelRscOuterClass;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgLstNodeOuterClass;
import com.linbit.linstor.proto.MsgLstRscDfnOuterClass;
import com.linbit.linstor.proto.MsgLstRscOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolDfnOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolOuterClass;
import com.linbit.linstor.proto.apidata.NodeApiData;
import com.linbit.linstor.proto.apidata.RscApiData;
import com.linbit.linstor.proto.apidata.RscDfnApiData;
import com.linbit.linstor.proto.apidata.StorPoolApiData;
import com.linbit.linstor.proto.apidata.StorPoolDfnApiData;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntDelVlmOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntPrimaryOuterClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class ProtoInterComSerializer implements InterComSerializer {
    private final ErrorReporter errReporter;

    public ProtoInterComSerializer()
    {
        this.errReporter = null;
    }

    public ProtoInterComSerializer(final ErrorReporter errReporter)
    {
        this.errReporter = errReporter;
    }

    @Override
    public InterComBuilder builder(String apiCall, int msgId) {
        return new ProtoInterComBuilder(errReporter, apiCall, msgId);
    }
}

class ProtoInterComBuilder implements InterComBuilder {
    private ByteArrayOutputStream baos;
    final private ErrorReporter errReporter;

    public ProtoInterComBuilder(final ErrorReporter errReporter, String apiCall, int msgId) {
        this.errReporter = errReporter;
        baos = new ByteArrayOutputStream();

        try {
            MsgHeaderOuterClass.MsgHeader.newBuilder()
                .setApiCall(apiCall)
                .setMsgId(msgId)
                .build()
                .writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
    }

    @Override
    public byte[] build()
    {
        return baos.toByteArray();
    }

    @Override
    public InterComBuilder primaryRequest(String rscName, String rscUuid) {
        try {
            MsgIntPrimaryOuterClass.MsgIntPrimary.Builder msgReqPrimary = MsgIntPrimaryOuterClass.MsgIntPrimary.newBuilder();
            msgReqPrimary.setRscName(rscName);
            msgReqPrimary.setRscUuid(rscUuid);

            msgReqPrimary.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
        return this;
    }

    @Override
    public InterComBuilder nodeList(List<Node.NodeApi> nodes) {
        MsgLstNodeOuterClass.MsgLstNode.Builder msgListNodeBuilder = MsgLstNodeOuterClass.MsgLstNode.newBuilder();

        try {
            for (Node.NodeApi apiNode: nodes)
            {
                msgListNodeBuilder.addNodes(NodeApiData.toNodeProto(apiNode));
            }

            msgListNodeBuilder.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
        return this;
    }

    @Override
    public InterComBuilder storPoolDfnList(List<StorPoolDefinition.StorPoolDfnApi> storpooldfns) {
        MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.Builder msgListStorPoolDfnsBuilder =
                MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.newBuilder();

        for (StorPoolDefinition.StorPoolDfnApi apiDfn: storpooldfns)
        {
            msgListStorPoolDfnsBuilder.addStorPoolDfns(StorPoolDfnApiData.fromStorPoolDfnApi(apiDfn));
        }

        try {
            msgListStorPoolDfnsBuilder.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
        return this;
    }

    @Override
    public InterComBuilder storPoolList(List<StorPool.StorPoolApi> storpools) {
        MsgLstStorPoolOuterClass.MsgLstStorPool.Builder msgListBuilder =
                MsgLstStorPoolOuterClass.MsgLstStorPool.newBuilder();
        for (StorPool.StorPoolApi apiStorPool: storpools)
        {
            msgListBuilder.addStorPools(StorPoolApiData.toStorPoolProto(apiStorPool));
        }

        try {
            msgListBuilder.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }

        return this;
    }

    @Override
    public InterComBuilder resourceDfnList(List<ResourceDefinition.RscDfnApi> rscDfns) {
        MsgLstRscDfnOuterClass.MsgLstRscDfn.Builder msgListRscDfnsBuilder = MsgLstRscDfnOuterClass.MsgLstRscDfn.newBuilder();

        for (ResourceDefinition.RscDfnApi apiRscDfn: rscDfns)
        {
            msgListRscDfnsBuilder.addRscDfns(RscDfnApiData.fromRscDfnApi(apiRscDfn));
        }

        try {
            msgListRscDfnsBuilder.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
        return this;
    }

    @Override
    public InterComBuilder resourceList(List<Resource.RscApi> rscs) {
        MsgLstRscOuterClass.MsgLstRsc.Builder msgListRscsBuilder = MsgLstRscOuterClass.MsgLstRsc.newBuilder();

        for (Resource.RscApi apiRsc: rscs)
        {
            msgListRscsBuilder.addResources(RscApiData.toRscProto(apiRsc));
        }

        try {
            msgListRscsBuilder.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }

        return this;
    }

    @Override
    public InterComBuilder notifyResourceDeleted(String nodeName, String resourceName, String rscUuid) {
        MsgDelRscOuterClass.MsgDelRsc.Builder msgDelRscBld = MsgDelRscOuterClass.MsgDelRsc.newBuilder();
        msgDelRscBld.setNodeName(nodeName);
        msgDelRscBld.setRscName(resourceName);
        msgDelRscBld.setUuid(rscUuid);

        try {
            msgDelRscBld.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }

        return this;
    }

    @Override
    public InterComBuilder notifyVolumeDeleted(String nodeName, String resourceName, int volumeNr) {
        MsgIntDelVlmOuterClass.MsgIntDelVlm.Builder msgDelVlmBld = MsgIntDelVlmOuterClass.MsgIntDelVlm.newBuilder();
        msgDelVlmBld.setNodeName(nodeName);
        msgDelVlmBld.setRscName(resourceName);
        msgDelVlmBld.setVlmNr(volumeNr);

        try {
            msgDelVlmBld.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }

        return this;
    }

    @Override
    public InterComBuilder authMessage(UUID nodeUuid, String nodeName, byte[] sharedSecret) {
        try {
            MsgIntAuthOuterClass.MsgIntAuth.newBuilder()
                    .setNodeUuid(nodeUuid.toString())
                    .setNodeName(nodeName)
                    .setSharedSecret(ByteString.copyFrom(sharedSecret))
                    .build()
                    .writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }

        return this;
    }
}
