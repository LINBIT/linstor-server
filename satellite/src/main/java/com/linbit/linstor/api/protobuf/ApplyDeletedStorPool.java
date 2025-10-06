package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedStorPoolOuterClass.MsgIntApplyDeletedStorPool;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_STOR_POOL_DELETED,
    description = "Applies an update of a deleted storage pool (ensuring the storage pool is deleted)"
)
@Singleton
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
        MsgIntApplyDeletedStorPool storPoolDeletedData = MsgIntApplyDeletedStorPool.parseDelimitedFrom(msgDataIn);
        apiCallHandler.applyDeletedStorPoolChange(
            storPoolDeletedData.getNodeName(),
            storPoolDeletedData.getStorPoolName(),
            storPoolDeletedData.getFullSyncId(),
            storPoolDeletedData.getUpdateId()
        );
    }
}
