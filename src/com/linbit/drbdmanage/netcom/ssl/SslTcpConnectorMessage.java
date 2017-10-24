package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage;

public class SslTcpConnectorMessage extends TcpConnectorMessage
{
    private SSLEngine sslEngine;
    private ByteBuffer encryptedBuffer;
    private ByteBuffer decryptedBuffer;
    private SslTcpConnectorPeer peer;

    protected SslTcpConnectorMessage(
        final boolean forSend,
        final SSLEngine sslEngine,
        final SslTcpConnectorPeer peer
    )
    {
        super(forSend);
        this.sslEngine = sslEngine;
        this.peer = peer;
        encryptedBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
        decryptedBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getApplicationBufferSize());
        decryptedBuffer.flip(); // pretend that we had a last read or write op, which has consumed all bytes
        encryptedBuffer.flip(); // same as above
    }

    @Override
    protected ReadState read(SocketChannel inChannel) throws IllegalMessageStateException, IOException
    {
        ReadState state;
        int encryptedPos = 0;
        do
        {
            encryptedPos = encryptedBuffer.position();
            state = super.read(inChannel);
        }
        while (state != ReadState.FINISHED && encryptedPos != encryptedBuffer.position());

        return state;
    }

    @Override
    protected int read(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        int read;
        if (needsHandshaking(channel))
        {
            // Message was used for handshake, no data was read
            read = 0;
        }
        else
        {
            int encryptedRead = 0;
            if (!decryptedBuffer.hasRemaining())
            {
                SSLEngineResult result;
                do
                {
                    decryptedBuffer.clear();
                    if (!encryptedBuffer.hasRemaining())
                    {
                        encryptedBuffer.clear();
                        encryptedRead = channel.read(encryptedBuffer);

                        if (encryptedRead < 0)
                        {
                            break;
                        }
                        encryptedBuffer.flip();
                    }
                    result = sslEngine.unwrap(encryptedBuffer, decryptedBuffer);
                    if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                    {
                        decryptedBuffer = ByteBuffer.allocateDirect(decryptedBuffer.capacity() * 2);
                    }
                }
                while (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW);
                decryptedBuffer.flip();
            }
            if (encryptedRead >= 0)
            {
                if (buffer.hasArray() && decryptedBuffer.hasArray())
                {
                    final int origDecryptLimit = decryptedBuffer.limit();
                    final int origDecryptPos = decryptedBuffer.position();
                    final int origBufferLimit = buffer.limit();
                    final int origBufferPos = buffer.position();

                    final byte[] decryptArr = decryptedBuffer.array();
                    final byte[] bufferArr = buffer.array();

                    read = Math.min(origBufferLimit - origBufferPos, origDecryptLimit - origDecryptPos);

                    System.arraycopy(decryptArr, origDecryptPos, bufferArr, origBufferPos, read);

                    decryptedBuffer.position(origDecryptPos + read);
                    buffer.position(origBufferPos + read);
                }
                else
                {
                    // FIXME: Hopefully both are DirectBuffers...
                    int oldLimit = decryptedBuffer.limit();
                    int oldPos = decryptedBuffer.position();
                    if (decryptedBuffer.remaining() > buffer.remaining())
                    {
                        decryptedBuffer.limit(decryptedBuffer.position() + buffer.remaining());
                    }
                    buffer.put(decryptedBuffer);
                    decryptedBuffer.limit(oldLimit);
                    read = decryptedBuffer.position() - oldPos;
                }
            }
            else
            {
                read = -1; // encrypted read was -1, so we will return that too
            }
        }
        return read;
    }

    @Override
    protected boolean write(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        boolean flushed;
        if (needsHandshaking(channel))
        {
            flushed = false;
        }
        else
        {
            if (!encryptedBuffer.hasRemaining())
            {
                encryptedBuffer.clear();
                sslEngine.wrap(buffer, encryptedBuffer);
                encryptedBuffer.flip();
            }
            channel.write(encryptedBuffer);
            flushed = !buffer.hasRemaining() && !encryptedBuffer.hasRemaining();
        }
        return flushed;
    }

    private boolean needsHandshaking(SocketChannel channel) throws IOException
    {
        return !peer.doHandshake(channel, sslEngine);
    }
}
