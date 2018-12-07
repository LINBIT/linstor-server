package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModRscDfnOuterClass.MsgModRscDfn;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_RSC_DFN,
    description = "Modifies a resource definition"
)
@Singleton
public class ModifyResourceDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyResourceDefinition(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgModRscDfn modRscDfn = MsgModRscDfn.parseDelimitedFrom(msgDataIn);
        UUID rscDfnUUID = null;
        if (modRscDfn.hasRscDfnUuid())
        {
            rscDfnUUID = UUID.fromString(modRscDfn.getRscDfnUuid());
        }
        String rscNameStr = modRscDfn.getRscName();
        Integer port = null;
        if (modRscDfn.hasRscDfnPort())
        {
            port = modRscDfn.getRscDfnPort();
        }
        Map<String, String> overrideProps = ProtoMapUtils.asMap(modRscDfn.getOverridePropsList());
        Set<String> delProps = new HashSet<>(modRscDfn.getDeletePropKeysList());
        ApiCallRc apiCallRc = apiCallHandler.modifyRscDfn(
            rscDfnUUID,
            rscNameStr,
            port,
            overrideProps,
            delProps
        );

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
