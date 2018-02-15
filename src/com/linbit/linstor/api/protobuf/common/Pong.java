package com.linbit.linstor.api.protobuf.common;

import com.google.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.netcom.Peer;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = "Pong",
    description = "Updates the Pong-received timestamp"
)
public class Pong implements ApiCall
{
    private final Peer peer;

    @Inject
    public Pong(Peer peerRef)
    {
        peer = peerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        peer.pongReceived();
    }

}
