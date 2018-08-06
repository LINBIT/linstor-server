package com.linbit.linstor.api.protobuf.common;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.netcom.Peer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple Ping request
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@ProtobufApiCall(
    name = "Ping",
    description = "Ping: Communication test. Responds with a Pong message."
)
public class Ping implements ApiCall
{
    private final ApiCallAnswerer apiCallAnswerer;
    private final Peer peer;

    @Inject
    public Ping(ApiCallAnswerer apiCallAnswererRef, Peer peerRef)
    {
        apiCallAnswerer = apiCallAnswererRef;
        peer = peerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
        apiCallAnswerer.writeAnswerHeader(dataOut, "Pong");
        peer.sendMessage(dataOut.toByteArray());
    }
}
