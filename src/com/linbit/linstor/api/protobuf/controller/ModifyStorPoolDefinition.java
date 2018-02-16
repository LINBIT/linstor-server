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
import com.linbit.linstor.proto.MsgModStorPoolDfnOuterClass.MsgModStorPoolDfn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyStorPoolDefinition extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyStorPoolDefinition(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_STOR_POOL_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a storage pool definition";
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
        MsgModStorPoolDfn msgModStorPoolDfn = MsgModStorPoolDfn.parseDelimitedFrom(msgDataIn);
        UUID storPoolDfnUuid = null;
        if (msgModStorPoolDfn.hasStorPoolDfnUuid())
        {
            storPoolDfnUuid = UUID.fromString(msgModStorPoolDfn.getStorPoolDfnUuid());
        }
        String storPoolName = msgModStorPoolDfn.getStorPoolName();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModStorPoolDfn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModStorPoolDfn.getDeletePropKeysList());

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyStorPoolDfn(
            accCtx,
            client,
            storPoolDfnUuid,
            storPoolName,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
