package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnDeleteApiCallHandler;
import com.linbit.linstor.proto.requests.MsgDelRscDfnOuterClass.MsgDelRscDfn;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_RSC_DFN,
    description = "Deletes a resource definition"
)
@Singleton
public class DeleteResourceDefinition implements ApiCallReactive
{
    private final CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public DeleteResourceDefinition(
        CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlRscDfnDeleteApiCallHandler = ctrlRscDfnDeleteApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgDelRscDfn msgDeleteRscDfn = MsgDelRscDfn.parseDelimitedFrom(msgDataIn);
        return ctrlRscDfnDeleteApiCallHandler
            .deleteResourceDefinition(msgDeleteRscDfn.getRscName())
            .transform(responseSerializer::transform);
    }
}
