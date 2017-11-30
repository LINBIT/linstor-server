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
import com.linbit.utils.UuidUtils;

@ProtobufApiCall
public class RequestStorPool extends BaseProtoApiCall
{
    private Controller controller;

    public RequestStorPool(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_REQUEST_STOR_POOL;
    }

    @Override
    public String getDescription()
    {
        return "This request is answered with a full data response of the requested storpool";
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
        MsgIntObjectId storPoolId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID storPoolUuid = UuidUtils.asUuid(storPoolId.getUuid().toByteArray());
        String storPoolName = storPoolId.getName();

        controller.getApiCallHandler().requestStorPool(satellitePeer, msgId, storPoolName, storPoolUuid);
    }
}
