package com.linbit.linstor.api.protobuf.internal;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

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
        UUID rscUuid = UUID.fromString(rscId.getUuid());
        String rscName = rscId.getName();

        rscInternalCallHandler.handleResourceRequest(nodeName, rscName);
    }
}
