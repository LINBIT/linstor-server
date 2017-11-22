package com.linbit.linstor.api.protobuf.common;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.CoreServices;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class Pong extends BaseProtoApiCall
{
    public Pong(CoreServices coreServices)
    {
        super(coreServices.getErrorReporter());
    }

    @Override
    public String getName()
    {
        return Pong.class.getSimpleName();
    }

    @Override
    public String getDescription()
    {
        return "Updates the Pong-received timestamp";
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
        client.pongReceived();
    }

}
