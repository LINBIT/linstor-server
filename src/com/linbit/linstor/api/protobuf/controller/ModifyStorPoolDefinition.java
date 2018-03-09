package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModStorPoolDfnOuterClass.MsgModStorPoolDfn;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_STOR_POOL_DFN,
    description = "Modifies a storage pool definition"
)
public class ModifyStorPoolDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyStorPoolDefinition(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
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

        ApiCallRc apiCallRc = apiCallHandler.modifyStorPoolDfn(
            storPoolDfnUuid,
            storPoolName,
            overrideProps,
            deletePropKeys
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
