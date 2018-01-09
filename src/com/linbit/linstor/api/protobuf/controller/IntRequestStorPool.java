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
public class IntRequestStorPool extends BaseProtoApiCall
{
    private Controller controller;

    public IntRequestStorPool(Controller controller)
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
        return "Called by the satellite to request storage pool update data";
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
        UUID storPoolUuid = UUID.fromString(storPoolId.getUuid());
        String storPoolName = storPoolId.getName();

        controller.getApiCallHandler().handleStorPoolRequest(satellitePeer, msgId, storPoolUuid, storPoolName);
    }
}
