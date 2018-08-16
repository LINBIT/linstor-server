package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiCallHandler;
import com.linbit.linstor.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.linstor.proto.RscOuterClass.Rsc;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.apidata.VlmApiData;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_RSC,
    description = "Creates a resource from a resource definition and assigns it to a node"
)
@Singleton
public class CreateResource implements ApiCallReactive
{
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public CreateResource(
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtRsc msgCrtRsc = MsgCrtRsc.parseDelimitedFrom(msgDataIn);
        Rsc rsc = msgCrtRsc.getRsc();

        List<VlmApi> vlmApiDataList = new ArrayList<>();
        for (Vlm vlm : rsc.getVlmsList())
        {
            vlmApiDataList.add(new VlmApiData(vlm));
        }

        return ctrlRscCrtApiCallHandler
            .createResource(
                rsc.getNodeName(),
                rsc.getName(),
                rsc.getRscFlagsList(),
                ProtoMapUtils.asMap(rsc.getPropsList()),
                vlmApiDataList,
                msgCrtRsc.getOverrideNodeId() ? rsc.getNodeId() : null
            )
            .transform(responseSerializer::transform);
    }
}
