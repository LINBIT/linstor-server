package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModRscDfnOuterClass.MsgModRscDfn;
import com.linbit.linstor.security.AccessContext;
import com.linbit.utils.UuidUtils;

@ProtobufApiCall
public class ModifyResourceDefinition extends BaseProtoApiCall
{
    private Controller controller;

    public ModifyResourceDefinition(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_RSC_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Modifies an existing resource definition";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgModRscDfn modRscDfn = MsgModRscDfn.parseDelimitedFrom(msgDataIn);
        UUID rscDfnUUID = null;
        if (modRscDfn.hasRscDfnUuid())
        {
            rscDfnUUID = UuidUtils.asUuid(modRscDfn.getRscDfnUuid().toByteArray());
        }
        String rscNameStr = modRscDfn.getRscName();
        Integer port = null;
        if (modRscDfn.hasRscDfnPort())
        {
            port = modRscDfn.getRscDfnPort();
        }
        Map<String, String> overrideProps = asMap(modRscDfn.getOverrideRscDfnPropsList());
        Set<String> delProps = new HashSet<>(modRscDfn.getDeleteRscDfnPropKeysList());

        controller.getApiCallHandler().modifyRscDfn(
            accCtx,
            client,
            rscDfnUUID,
            rscNameStr,
            port,
            overrideProps,
            delProps
        );
    }

}
