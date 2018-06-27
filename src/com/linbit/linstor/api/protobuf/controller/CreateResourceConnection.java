package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtRscConnOuterClass.MsgCrtRscConn;
import com.linbit.linstor.proto.RscConnOuterClass.RscConn;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_RSC_CONN,
    description = "Defines resource connection options"
)
public class CreateResourceConnection implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateResourceConnection(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtRscConn msgCreateRscConn = MsgCrtRscConn.parseDelimitedFrom(msgDataIn);
        RscConn rscConn = msgCreateRscConn.getRscConn();
        ApiCallRc apiCallRc = apiCallHandler.createResourceConnection(
            rscConn.getNodeName1(),
            rscConn.getNodeName2(),
            rscConn.getRscName(),
            ProtoMapUtils.asMap(rscConn.getRscConnPropsList())
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
