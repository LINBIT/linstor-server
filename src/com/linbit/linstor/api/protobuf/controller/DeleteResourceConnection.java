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
import com.linbit.linstor.proto.MsgDelRscConnOuterClass.MsgDelRscConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class DeleteResourceConnection extends BaseProtoApiCall
{
    private final Controller controller;

    public DeleteResourceConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_RSC_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes resource connection options";
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
        MsgDelRscConn msgDeleteRscConn = MsgDelRscConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteResourceConnection(
            accCtx,
            client,
            msgDeleteRscConn.getNodeName1(),
            msgDeleteRscConn.getNodeName2(),
            msgDeleteRscConn.getResourceName()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
