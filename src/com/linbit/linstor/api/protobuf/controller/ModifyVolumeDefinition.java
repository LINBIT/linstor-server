package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModVlmDfnOuterClass.MsgModVlmDfn;

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
public class ModifyVolumeDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyVolumeDefinition(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
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

        ApiCallRc apiCallRc = apiCallHandler.modifyVlmDfn(
            vlmDfnUuid,
            rscName,
            vlmNr,
            size,
            minorNr,
            overrideProps,
            deletePropKeys
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
