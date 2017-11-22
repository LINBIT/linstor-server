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
import com.linbit.linstor.proto.MsgCrtRscConnOuterClass.MsgCrtRscConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CreateResourceConnection extends BaseProtoApiCall
{
    private final Controller controller;

    public CreateResourceConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_RSC_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Defines resource connection options";
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
        MsgCrtRscConn msgCreateRscConn = MsgCrtRscConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().createResourceConnection(
            accCtx,
            client,
            msgCreateRscConn.getNodeName1(),
            msgCreateRscConn.getNodeName2(),
            msgCreateRscConn.getRscName(),
            asMap(msgCreateRscConn.getRscConnPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
