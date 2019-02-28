package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.proto.requests.MsgDelRscOuterClass.MsgDelRsc;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_RSC,
    description = "Deletes a resource"
)
@Singleton
public class DeleteResource implements ApiCallReactive
{
    private final CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public DeleteResource(
        CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlRscDeleteApiCallHandler = ctrlRscDeleteApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgDelRsc msgDeleteRsc = MsgDelRsc.parseDelimitedFrom(msgDataIn);
        return ctrlRscDeleteApiCallHandler
            .deleteResource(
                msgDeleteRsc.getNodeName(),
                msgDeleteRsc.getRscName()
            )
            .transform(responseSerializer::transform);
    }
}
