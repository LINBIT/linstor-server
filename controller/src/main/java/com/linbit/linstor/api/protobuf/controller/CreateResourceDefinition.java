package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoLayerUtils;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgCrtRscDfnOuterClass.MsgCrtRscDfn;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfn;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfnLayerData;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.apidata.VlmDfnWithPayloadApiData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_RSC_DFN,
    description = "Creates a resource definition"
)
@Singleton
public class CreateResourceDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateResourceDefinition(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtRscDfn msgCreateRscDfn = MsgCrtRscDfn.parseDelimitedFrom(msgDataIn);
        RscDfn rscDfn = msgCreateRscDfn.getRscDfn();

        List<VolumeDefinition.VlmDfnWtihCreationPayload> vlmDfnApiList = new ArrayList<>();
        for (final VlmDfn vlmDfn : rscDfn.getVlmDfnsList())
        {
            vlmDfnApiList.add(new VlmDfnWithPayloadApiData(vlmDfn));
        }

        List<RscDfnLayerData> layerDataList = rscDfn.getLayerDataList();
        // currently we ignore the possible payload, only extract the layer-stack
        List<String> layerStack = new ArrayList<>();
        for (RscDfnLayerData rscDfnData : layerDataList)
        {
            layerStack.add(ProtoLayerUtils.layerType2layerString(rscDfnData.getLayerType()));
        }

        ApiCallRc apiCallRc = apiCallHandler.createResourceDefinition(
            rscDfn.getRscName(),
            rscDfn.getExternalName().toByteArray(),
            msgCreateRscDfn.hasDrbdPort() ? msgCreateRscDfn.getDrbdPort() : null,
            msgCreateRscDfn.hasDrbdSecret() ? msgCreateRscDfn.getDrbdSecret() : null,
            msgCreateRscDfn.hasDrbdTransportType() ? msgCreateRscDfn.getDrbdTransportType() : null,
            ProtoMapUtils.asMap(rscDfn.getRscDfnPropsList()),
            vlmDfnApiList,
            layerStack
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
