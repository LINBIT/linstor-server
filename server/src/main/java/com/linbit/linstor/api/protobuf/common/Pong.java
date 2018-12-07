package com.linbit.linstor.api.protobuf.common;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.netcom.Peer;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = "Pong",
    description = "Updates the Pong-received timestamp",
    transactional = false
)
@Singleton
public class Pong implements ApiCall
{
    private final Provider<Peer> peerProvider;

    @Inject
    public Pong(Provider<Peer> peerProviderRef)
    {
        peerProvider = peerProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        peerProvider.get().pongReceived();
    }

}
