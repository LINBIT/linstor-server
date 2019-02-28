package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnModifyApiCallHandler;
import com.linbit.linstor.proto.requests.MsgModVlmDfnOuterClass.MsgModVlmDfn;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_VLM_DFN,
    description = "Modifies a volume definition"
)
@Singleton
public class ModifyVolumeDefinition implements ApiCallReactive
{
    private final CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ModifyVolumeDefinition(
        CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlVlmDfnModifyApiCallHandler = ctrlVlmDfnModifyApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgModVlmDfn msgModVlmDfn = MsgModVlmDfn.parseDelimitedFrom(msgDataIn);
        UUID vlmDfnUuid = null;
        if (msgModVlmDfn.hasVlmDfnUuid())
        {
            vlmDfnUuid = UUID.fromString(msgModVlmDfn.getVlmDfnUuid());
        }
        String rscName = msgModVlmDfn.getRscName();
        int vlmNr = msgModVlmDfn.getVlmNr();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModVlmDfn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModVlmDfn.getDeletePropKeysList());

        Long size = msgModVlmDfn.hasVlmSize() ? msgModVlmDfn.getVlmSize() : null;
        Integer minorNr = msgModVlmDfn.hasVlmMinor() ? msgModVlmDfn.getVlmMinor() : null;

        return ctrlVlmDfnModifyApiCallHandler
            .modifyVlmDfn(
                vlmDfnUuid,
                rscName,
                vlmNr,
                size,
                minorNr,
                overrideProps,
                deletePropKeys
            )
            .transform(responseSerializer::transform);
    }
}
