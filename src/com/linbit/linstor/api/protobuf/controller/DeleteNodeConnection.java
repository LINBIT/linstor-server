package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn;
import com.linbit.linstor.security.AccessContext;

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
