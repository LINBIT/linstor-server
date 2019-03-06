package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgCrtVlmDfnOuterClass.MsgCrtVlmDfn;
import com.linbit.linstor.proto.requests.MsgCrtVlmDfnOuterClass.VlmDfnWithPayload;
import com.linbit.utils.Pair;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.apidata.VlmDfnApiData;
import com.linbit.linstor.proto.apidata.VlmDfnWithPayloadApiData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_VLM_DFN,
    description = "Creates a volume definition"
)
@Singleton
public class CreateVolumeDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateVolumeDefinition(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtVlmDfn msgCrtVlmDfn = MsgCrtVlmDfn.parseDelimitedFrom(msgDataIn);

        List<VolumeDefinition.VlmDfnWtihCreationPayload> vlmDfnWithCrtPayloadApiList = new ArrayList<>();
        for (final VlmDfnWithPayload vlmDfnWithPayload : msgCrtVlmDfn.getVlmDfnsList())
        {
            vlmDfnWithCrtPayloadApiList.add(new VlmDfnWithPayloadApiData(vlmDfnWithPayload));
        }
        ApiCallRc apiCallRc = apiCallHandler.createVlmDfns(
            msgCrtVlmDfn.getRscName(),
            vlmDfnWithCrtPayloadApiList
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
