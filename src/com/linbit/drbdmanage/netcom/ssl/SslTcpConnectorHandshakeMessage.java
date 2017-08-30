package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import com.linbit.ImplementationError;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage;

public class SslTcpConnectorHandshakeMessage extends TcpConnectorMessage
{
    private SSLEngine sslEngine;
    private SslTcpConnectorPeer peer;

    protected SslTcpConnectorHandshakeMessage(
        final boolean forSend,
        final SslTcpConnectorPeer peer
    )
    {
        super(forSend);
        this.peer = peer;
    }

    void setSslEngine(SSLEngine sslEngine)
    {
        this.sslEngine = sslEngine;
    }

    @Override
    protected ReadState read(SocketChannel inChannel) throws IllegalMessageStateException, IOException
    {
        peer.doHandshake(inChannel, sslEngine);
        /*
         *  we must not return FINISHED here, as the message would then get processed (causing an
         *  IllegalMessageStateException as this ssl-handshaking message is not valid for the
         *  MessageProcessor).
         *  However, when handshaking is finished, it creates a new (not-handshaking, but ssl-decrypting)
         *  message which then can be marked as FINISHED and passed to the MessageProcessor (see class
         *  SslTcpConnectorMessage)
         */
        return ReadState.UNFINISHED;
    }

    @Override
    protected WriteState write(SocketChannel outChannel) throws IllegalMessageStateException, IOException
    {
        peer.doHandshake(outChannel, sslEngine);

        /*
         *  we do not have to return FINISHED here, as when handshaking is finished, it creates a new
         *  (not-handshaking, but ssl-encrypting) message (see class SslTcpConnectorMessage)
         */
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
}
