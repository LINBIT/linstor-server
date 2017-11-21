package com.linbit.drbdmanage.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.api.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtNodeConnOuterClass.MsgCrtNodeConn;
import com.linbit.drbdmanage.security.AccessContext;

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
        ApiCallRc apiCallRc = controller.getApiCallHandler().createNodeConnection(
            accCtx,
            client,
            // ignore nodeConnUuid
            msgCreateNodeConn.getNodeName1(),
            msgCreateNodeConn.getNodeName2(),
            asMap(msgCreateNodeConn.getNodeConnPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
