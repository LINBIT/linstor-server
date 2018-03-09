package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtVlmConnOuterClass.MsgCrtVlmConn;
import com.linbit.linstor.proto.VlmConnOuterClass.VlmConn;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_VLM_CONN,
    description = "Defines volume connection options"
)
public class CreateVolumeConnection implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateVolumeConnection(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtVlmConn msgCreateVlmConn = MsgCrtVlmConn.parseDelimitedFrom(msgDataIn);
        VlmConn vlmConn = msgCreateVlmConn.getVlmConn();

        ApiCallRc apiCallRc = apiCallHandler.createVolumeConnection(
            vlmConn.getNodeName1(),
            vlmConn.getNodeName2(),
            vlmConn.getResourceName(),
            vlmConn.getVolumeNr(),
            ProtoMapUtils.asMap(vlmConn.getVolumeConnPropsList())
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
