package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModStorPoolOuterClass.MsgModStorPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_STOR_POOL,
    description = "Modifies a storage pool"
)
public class ModifyStorPool implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyStorPool(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgModStorPool msgModStorPool = MsgModStorPool.parseDelimitedFrom(msgDataIn);
        UUID storPoolUuid = null;
        if (msgModStorPool.hasStorPoolUuid())
        {
            storPoolUuid = UUID.fromString(msgModStorPool.getStorPoolUuid());
        }
        String nodeName = msgModStorPool.getNodeName();
        String storPoolName = msgModStorPool.getStorPoolName();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModStorPool.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModStorPool.getDeletePropKeysList());

        ApiCallRc apiCallRc = apiCallHandler.modifyStorPool(
            storPoolUuid,
            nodeName,
            storPoolName,
            overrideProps,
            deletePropKeys
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
