package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgDelVlmConnOuterClass.MsgDelVlmConn;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_VLM_CONN,
    description = "Deletes volume connection options"
)
@Singleton
public class DeleteVolumeConnection implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public DeleteVolumeConnection(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgDelVlmConn msgDeleteVlmConn = MsgDelVlmConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.deleteVolumeConnection(
            msgDeleteVlmConn.getNodeName1(),
            msgDeleteVlmConn.getNodeName2(),
            msgDeleteVlmConn.getResourceName(),
            msgDeleteVlmConn.getVolumeNr()
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
