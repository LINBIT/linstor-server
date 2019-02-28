package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgCrtVlmConnOuterClass.MsgCrtVlmConn;
import com.linbit.linstor.proto.common.VlmConnOuterClass.VlmConn;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_VLM_CONN,
    description = "Defines volume connection options"
)
@Singleton
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
