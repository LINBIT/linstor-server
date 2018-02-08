package com.linbit.linstor.api.protobuf.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.CoreServices;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

/**
 * Simple Ping request
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@ProtobufApiCall
public class Ping extends BaseProtoApiCall
{

    @Override
    public String getName()
    {
        return Ping.class.getSimpleName();
    }

    @Override
    public String getDescription()
    {
        return "Ping: Communication test. Responds with a Pong message.";
    }

    public Ping(CoreServices coreSvcsRef)
    {
        super(coreSvcsRef.getErrorReporter());
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
        ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
        writeProtoMsgHeader(dataOut, msgId, "Pong");
        client.sendMessage(dataOut.toByteArray());
    }
}
