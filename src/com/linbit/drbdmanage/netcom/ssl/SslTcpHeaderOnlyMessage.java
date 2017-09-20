package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.TcpHeaderOnlyMessage;

public class SslTcpHeaderOnlyMessage extends TcpHeaderOnlyMessage
{
    private SSLEngine sslEngine;
    private ByteBuffer encryptedBuffer;

    protected SslTcpHeaderOnlyMessage(int type)
    {
        super(type);
    }

    void setSslEngine(SSLEngine sslEngineRef)
    {
        sslEngine = sslEngineRef;
        encryptedBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
    }

    @Override
    protected WriteState write(SocketChannel outChannel) throws IllegalMessageStateException, IOException
    {
        WriteState state;
        synchronized (this)
        {
            if (!encryptedBuffer.hasRemaining())
            {
                sslEngine.wrap(byteBuffer, encryptedBuffer);
                encryptedBuffer.flip();
                byteBuffer.flip();
            }
            outChannel.write(encryptedBuffer);
            if (!encryptedBuffer.hasRemaining())
            {
                encryptedBuffer.clear();
                state = WriteState.FINISHED;
            }
            else
            {
                state = WriteState.UNFINISHED;
            }
        }
        return state;
    }
}
