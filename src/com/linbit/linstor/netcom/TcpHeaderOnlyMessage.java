package com.linbit.linstor.netcom;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import com.linbit.ImplementationError;

// TODO: if more than classes are needed beside Tcp(Ping|Pong)InternalMessage
// create a common super class for them
public class TcpHeaderOnlyMessage extends TcpConnectorMessage
{
    protected static final byte[] DATA = new byte[0];
    protected final ByteBuffer byteBuffer;

    protected TcpHeaderOnlyMessage(int type)
    {
        super(true);
        byte[] buffer = new byte[HEADER_SIZE];
        byteBuffer = ByteBuffer.wrap(buffer);
        byteBuffer.putInt(TYPE_FIELD_OFFSET, type);
    }

    @Override
    public byte[] getData()
    {
        return DATA;
    }

    @Override
    public void setData(byte[] data)
    {
        throw new ImplementationError("Cannot set Data of hardcoded Ping message", null);
    }

    @Override
    protected int read(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        throw new ImplementationError("TcpPingMessage should not receive read events", null);
    }

    @Override
    protected ReadState read(SocketChannel inChannel) throws IllegalMessageStateException, IOException
    {
        throw new ImplementationError("TcpPingMessage should not receive read events", null);
    }

    @Override
    protected WriteState write(SocketChannel outChannel) throws IllegalMessageStateException, IOException
    {
        WriteState state;
        synchronized (this)
        {
            outChannel.write(byteBuffer);
            if (byteBuffer.hasRemaining())
            {
                state = WriteState.UNFINISHED;
            }
            else
            {
                state = WriteState.FINISHED;
                byteBuffer.flip();
            }
        }
        return state;
    }

    @Override
    protected boolean write(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        throw new ImplementationError("TcpPingMessage should not receive bytes to write", null);
    }
}
