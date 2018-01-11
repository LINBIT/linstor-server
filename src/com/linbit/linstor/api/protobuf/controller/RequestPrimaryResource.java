package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntPrimaryOuterClass.MsgIntPrimary;
import com.linbit.linstor.security.AccessContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall
public class RequestPrimaryResource extends BaseProtoApiCall {
    private final Controller controller;

    public RequestPrimaryResource(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_REQUEST_PRIMARY_RSC;
    }

    @Override
    public String getDescription()
    {
        return "Satellite request primary for a resource";
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgIntPrimary msgReqPrimary = MsgIntPrimary.parseDelimitedFrom(msgDataIn);
        controller.getApiCallHandler().handlePrimaryResourceRequest(
            accCtx,
            client,
            msgId,
            msgReqPrimary.getRscName(),
            UUID.fromString(msgReqPrimary.getRscUuid())
        );
    }
}
