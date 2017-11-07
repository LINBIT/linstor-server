package com.linbit.drbdmanage.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscIdOuterClass.MsgIntRscId;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.utils.UuidUtils;

@ProtobufApiCall
public class RequestResource extends BaseProtoApiCall
{
    private Controller controller;

    public RequestResource(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_REQ_RSC;
    }

    @Override
    public String getDescription()
    {
        return "This request is answered with a full data response of the requested resource";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgIntRscId rscId = MsgIntRscId.parseDelimitedFrom(msgDataIn);
        UUID rscUuid = UuidUtils.asUuid(rscId.getUuid().toByteArray());
        String rscName = rscId.getResourceName();

        controller.getApiCallHandler().requestResource(rscName, rscUuid, msgId);

    }

}
