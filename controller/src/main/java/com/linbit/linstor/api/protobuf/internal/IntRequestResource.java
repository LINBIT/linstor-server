package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_RSC,
    description = "Called by the satellite to request resource update data",
    transactional = false
)
@Singleton
public class IntRequestResource implements ApiCall
{
    private final RscInternalCallHandler rscInternalCallHandler;

    @Inject
    public IntRequestResource(RscInternalCallHandler apiCallHandlerRef)
    {
        rscInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        IntObjectId nodeId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String nodeName = nodeId.getName();

        IntObjectId rscId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String rscName = rscId.getName();

        rscInternalCallHandler.handleResourceRequest(nodeName, rscName);
    }
}
