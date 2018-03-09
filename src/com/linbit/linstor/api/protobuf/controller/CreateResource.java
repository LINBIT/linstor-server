package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.linstor.proto.RscOuterClass.Rsc;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.apidata.VlmApiData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_RSC,
    description = "Creates a resource from a resource definition and assigns it to a node"
)
public class CreateResource implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateResource(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtRsc msgCrtRsc = MsgCrtRsc.parseDelimitedFrom(msgDataIn);
        Rsc rsc = msgCrtRsc.getRsc();

        List<VlmApi> vlmApiDataList = new ArrayList<>();
        for (Vlm vlm : rsc.getVlmsList())
        {
            vlmApiDataList.add(new VlmApiData(vlm));
        }

        ApiCallRc apiCallRc = apiCallHandler.createResource(
            rsc.getNodeName(),
            rsc.getName(),
            rsc.getRscFlagsList(),
            ProtoMapUtils.asMap(rsc.getPropsList()),
            vlmApiDataList
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
