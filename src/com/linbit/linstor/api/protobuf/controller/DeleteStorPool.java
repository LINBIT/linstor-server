package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgDelStorPoolOuterClass.MsgDelStorPool;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_STOR_POOL,
    description = "Deletes a storage pool name registration"
)
public class DeleteStorPool implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public DeleteStorPool(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgDelStorPool msgDeleteStorPool = MsgDelStorPool.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = apiCallHandler.deleteStoragePool(
            msgDeleteStorPool.getNodeName(),
            msgDeleteStorPool.getStorPoolName()
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
