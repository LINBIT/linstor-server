package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class IntRequestResource extends BaseProtoApiCall
{
    private Controller controller;

    public IntRequestResource(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_REQUEST_RSC;
    }

    @Override
    public String getDescription()
    {
        return "Called by the satellite to request resource update data";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer satellitePeer
    )
        throws IOException
    {
        MsgIntObjectId nodeId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        String nodeName = nodeId.getName();

        MsgIntObjectId rscId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID rscUuid = UUID.fromString(rscId.getUuid());
        String rscName = rscId.getName();

        controller.getApiCallHandler().handleResourceRequest(satellitePeer, msgId, nodeName, rscUuid, rscName);
    }
}
