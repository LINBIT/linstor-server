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
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModRscOuterClass.MsgModRsc;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyResource extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyResource(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_RSC;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a resource";
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
        MsgModRsc msgModRsc = MsgModRsc.parseDelimitedFrom(msgDataIn);
        UUID rscUuid = null;
        if (msgModRsc.hasRscUuid())
        {
            rscUuid = UUID.fromString(msgModRsc.getRscUuid());
        }
        String nodeName = msgModRsc.getNodeName();
        String rscName = msgModRsc.getRscName();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModRsc.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModRsc.getDeletePropKeysList());

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyRsc(
            accCtx,
            client,
            rscUuid,
            nodeName,
            rscName,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
