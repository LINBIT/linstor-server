package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtVlmDfnOuterClass.MsgCrtVlmDfn;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.apidata.VlmDfnApiData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_VLM_DFN,
    description = "Creates a volume definition"
)
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

        List<VlmDfnApi> vlmDfnApiList = new ArrayList<>();
        for (final VlmDfn vlmDfn : msgCrtVlmDfn.getVlmDfnsList())
        {
            vlmDfnApiList.add(new VlmDfnApiData(vlmDfn));
        }
        ApiCallRc apiCallRc = apiCallHandler.createVlmDfns(
            msgCrtVlmDfn.getRscName(),
            vlmDfnApiList
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
