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
import com.linbit.drbdmanage.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn;
import com.linbit.drbdmanage.security.AccessContext;

@ProtobufApiCall
public class DeleteNodeConnection extends BaseProtoApiCall
{
    private final Controller controller;

    public DeleteNodeConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_NODE_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes node connection options";
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
        MsgDelNodeConn msgDeleteNodeConn = MsgDelNodeConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteNodeConnection(
            accCtx,
            client,
            msgDeleteNodeConn.getNodeName1(),
            msgDeleteNodeConn.getNodeName2()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
