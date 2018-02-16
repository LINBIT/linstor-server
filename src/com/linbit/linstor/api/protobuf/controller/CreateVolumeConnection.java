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
import com.linbit.linstor.proto.MsgCrtVlmConnOuterClass.MsgCrtVlmConn;
import com.linbit.linstor.proto.VlmConnOuterClass.VlmConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CreateVolumeConnection extends BaseProtoApiCall
{
    private final Controller controller;

    public CreateVolumeConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_VLM_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Defines volume connection options";
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
        MsgCrtVlmConn msgCreateVlmConn = MsgCrtVlmConn.parseDelimitedFrom(msgDataIn);
        VlmConn vlmConn = msgCreateVlmConn.getVlmConn();

        ApiCallRc apiCallRc = controller.getApiCallHandler().createVolumeConnection(
            accCtx,
            client,
            vlmConn.getNodeName1(),
            vlmConn.getNodeName2(),
            vlmConn.getResourceName(),
            vlmConn.getVolumeNr(),
            ProtoMapUtils.asMap(vlmConn.getVolumeConnPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
