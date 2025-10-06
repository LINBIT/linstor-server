package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.StorPoolInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_STOR_POOL,
    description = "Called by the satellite to request storage pool update data",
    transactional = false
)
@Singleton
public class IntRequestStorPool implements ApiCall
{
    private final StorPoolInternalCallHandler storPoolInternalCallHandler;

    @Inject
    public IntRequestStorPool(StorPoolInternalCallHandler apiCallHandlerRef)
    {
        storPoolInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        IntObjectId nodeNameId = IntObjectId.parseDelimitedFrom(msgDataIn);
        IntObjectId storPoolId = IntObjectId.parseDelimitedFrom(msgDataIn);
        UUID storPoolUuid = ProtoUuidUtils.deserialize(storPoolId.getUuid());
        String storPoolName = storPoolId.getName();
        String nodeNameStr = nodeNameId.getName();

        storPoolInternalCallHandler.handleStorPoolRequest(storPoolUuid, nodeNameStr, storPoolName);
    }
}
