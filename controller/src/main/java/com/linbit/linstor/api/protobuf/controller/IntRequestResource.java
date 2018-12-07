package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

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
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public IntRequestResource(CtrlApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntObjectId nodeId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        String nodeName = nodeId.getName();

        MsgIntObjectId rscId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID rscUuid = UUID.fromString(rscId.getUuid());
        String rscName = rscId.getName();

        apiCallHandler.handleResourceRequest(nodeName, rscUuid, rscName);
    }
}
