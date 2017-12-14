package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModVlmDfnOuterClass.MsgModVlmDfn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyVolumeDefinition extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyVolumeDefinition(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_VLM_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a volume definition";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
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
        Map<String, String> overrideProps = asMap(msgModVlmDfn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModVlmDfn.getDeletePropKeysList());

        Long size = msgModVlmDfn.hasVlmSize() ? msgModVlmDfn.getVlmSize() : null;
        Integer minorNr = msgModVlmDfn.hasVlmMinor() ? msgModVlmDfn.getVlmMinor() : null;

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyVlmDfn(
            accCtx,
            client,
            vlmDfnUuid,
            rscName,
            vlmNr,
            size,
            minorNr,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
