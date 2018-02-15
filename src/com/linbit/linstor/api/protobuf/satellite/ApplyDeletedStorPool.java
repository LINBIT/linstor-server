package com.linbit.linstor.api.protobuf.satellite;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDeletedDataOuterClass.MsgIntStorPoolDeletedData;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_STOR_POOL_DELETED,
    description = "Applies an update of a deleted storage pool (ensuring the storage pool is deleted)"
)
public class ApplyDeletedStorPool implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyDeletedStorPool(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntStorPoolDeletedData storPoolDeletedData = MsgIntStorPoolDeletedData.parseDelimitedFrom(msgDataIn);
        apiCallHandler.applyDeletedStorPoolChange(
            storPoolDeletedData.getStorPoolName()
        );
    }
}
