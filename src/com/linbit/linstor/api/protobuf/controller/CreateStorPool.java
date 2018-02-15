package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
import com.linbit.linstor.proto.StorPoolOuterClass.StorPool;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_STOR_POOL,
    description = "Creates a storage pool name registration"
)
public class CreateStorPool implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateStorPool(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtStorPool msgCreateStorPool = MsgCrtStorPool.parseDelimitedFrom(msgDataIn);
        StorPool storPool = msgCreateStorPool.getStorPool();

        ApiCallRc apiCallRc = apiCallHandler.createStoragePool(
            storPool.getNodeName(),
            storPool.getStorPoolName(),
            storPool.getDriver(),
            ProtoMapUtils.asMap(storPool.getPropsList())
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
