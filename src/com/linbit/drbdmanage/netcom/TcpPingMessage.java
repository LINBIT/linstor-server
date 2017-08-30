package com.linbit.drbdmanage.netcom;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;

public class TcpPingMessage extends TcpConnectorMessage
{
    protected static final byte[] PING_CONTENT;
    protected static final ByteBuffer PING_BYTE_BUFFER;

    static
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            MsgHeader.Builder headerBuilder = MsgHeader.newBuilder();
            headerBuilder.setMsgId(42);
            headerBuilder.setApiCall("Ping");

            MsgHeader header = headerBuilder.build();
            header.writeDelimitedTo(baos);

            PING_CONTENT = baos.toByteArray();

            byte[] buffer = new byte[HEADER_SIZE + PING_CONTENT.length];
            System.arraycopy(PING_CONTENT, 0, buffer, HEADER_SIZE, PING_CONTENT.length);

            PING_BYTE_BUFFER = ByteBuffer.wrap(buffer);
            PING_BYTE_BUFFER.putInt(LENGTH_FIELD_OFFSET, PING_CONTENT.length);
        }
        catch (IOException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    protected TcpPingMessage()
    {
        super(true);
    }

    @Override
    public byte[] getData()
    {
        return Arrays.copyOf(PING_CONTENT, PING_CONTENT.length);
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
        synchronized (this)
        {
            outChannel.write(PING_BYTE_BUFFER);
            PING_BYTE_BUFFER.flip();
        }
        return WriteState.FINISHED;
    }

    @Override
    protected boolean write(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        throw new ImplementationError("TcpPingMessage should not receive bytes to write", null);
    }
}
