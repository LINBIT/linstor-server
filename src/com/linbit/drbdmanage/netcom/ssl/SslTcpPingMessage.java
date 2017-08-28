package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.TcpPingMessage;

public class SslTcpPingMessage extends TcpPingMessage
{
    private SSLEngine sslEngine;
    private final ByteBuffer encryptedBuffer;

    public SslTcpPingMessage(int packetBufferSize)
    {
        encryptedBuffer = ByteBuffer.allocateDirect(packetBufferSize);
    }

    void setSslEngine(SSLEngine sslEngine)
    {
        this.sslEngine = sslEngine;
    }

    @Override
    protected WriteState write(SocketChannel outChannel) throws IllegalMessageStateException, IOException
    {
        synchronized (this)
        {
            sslEngine.wrap(PING_BYTE_BUFFER, encryptedBuffer);
            encryptedBuffer.flip();
            PING_BYTE_BUFFER.flip();
            outChannel.write(encryptedBuffer);
            encryptedBuffer.clear();
        }
        return WriteState.FINISHED;
    }
}
