package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgCrtNodeConnOuterClass.MsgCrtNodeConn;
import com.linbit.linstor.proto.NodeConnOuterClass.NodeConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CreateNodeConnection extends BaseProtoApiCall
{
    private final Controller controller;

    public CreateNodeConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_NODE_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Defines node connection options";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgCrtNodeConn msgCreateNodeConn = MsgCrtNodeConn.parseDelimitedFrom(msgDataIn);
        NodeConn nodeConn = msgCreateNodeConn.getNodeConn();
        ApiCallRc apiCallRc = controller.getApiCallHandler().createNodeConnection(
            accCtx,
            client,
            // ignore nodeConnUuid
            nodeConn.getNodeName1(),
            nodeConn.getNodeName2(),
            ProtoMapUtils.asMap(nodeConn.getNodeConnPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
