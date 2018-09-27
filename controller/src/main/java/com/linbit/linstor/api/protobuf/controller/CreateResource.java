package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.Resource;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiCallHandler;
import com.linbit.linstor.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.linstor.proto.apidata.RscApiData;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

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

        List<Resource.RscApi> rscApiList = msgCrtRsc.getRscsList().stream()
            .map(RscApiData::new)
            .collect(Collectors.toList());

        return ctrlRscCrtApiCallHandler
            .createResource(rscApiList)
            .transform(responseSerializer::transform);
    }
}
