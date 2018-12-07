package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnDeleteApiCallHandler;
import com.linbit.linstor.proto.MsgDelVlmDfnOuterClass.MsgDelVlmDfn;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_DEL_VLM_DFN,
    description = "Deletes a volume definition"
)
@Singleton
public class DeleteVolumeDefinition implements ApiCallReactive
{
    private final CtrlVlmDfnDeleteApiCallHandler ctrlVlmDfnDeleteApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public DeleteVolumeDefinition(
        CtrlVlmDfnDeleteApiCallHandler ctrlVlmDfnDeleteApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlVlmDfnDeleteApiCallHandler = ctrlVlmDfnDeleteApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgDelVlmDfn msgDelVlmDfn = MsgDelVlmDfn.parseDelimitedFrom(msgDataIn);

        return ctrlVlmDfnDeleteApiCallHandler
            .deleteVolumeDefinition(
                msgDelVlmDfn.getRscName(),
                msgDelVlmDfn.getVlmNr()
            )
            .transform(responseSerializer::transform);
    }
}
