package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_STOR_POOL,
    description = "Called by the satellite to request storage pool update data"
)
public class IntRequestStorPool implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public IntRequestStorPool(CtrlApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntObjectId storPoolId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID storPoolUuid = UUID.fromString(storPoolId.getUuid());
        String storPoolName = storPoolId.getName();

        apiCallHandler.handleStorPoolRequest(storPoolUuid, storPoolName);
    }
}
