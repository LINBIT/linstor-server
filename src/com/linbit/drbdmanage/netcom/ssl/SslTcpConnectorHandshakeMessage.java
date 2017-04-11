package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage;

public class SslTcpConnectorHandshakeMessage extends TcpConnectorMessage
{
    private SSLEngine sslEngine;
    private SslTcpConnectorPeer peer;

    protected SslTcpConnectorHandshakeMessage(
        final boolean forSend,
        final SSLEngine sslEngine,
        final SslTcpConnectorPeer peer
    )
        throws SSLException
    {
        super(forSend);
        this.sslEngine = sslEngine;
        this.peer = peer;

        this.sslEngine.beginHandshake();
    }

    @Override
    protected ReadState read(SocketChannel inChannel) throws IllegalMessageStateException, IOException
    {
        peer.doHandshake(inChannel, sslEngine);
        return ReadState.UNFINISHED;
    }

    @Override
    protected WriteState write(SocketChannel outChannel) throws IllegalMessageStateException, IOException
    {
        peer.doHandshake(outChannel, sslEngine);
        return WriteState.UNFINISHED;
    }

    @Override
    protected int read(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        throw new ImplementationError("This method should never be called", new IllegalStateException());
    }

    @Override
    protected boolean write(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        throw new ImplementationError("This method should never be called", new IllegalStateException());
    }

    private boolean needsHandshaking(SocketChannel channel) throws IOException
    {
        return !peer.doHandshake(channel, sslEngine);
    }
}
