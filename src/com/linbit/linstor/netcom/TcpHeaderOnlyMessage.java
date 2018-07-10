package com.linbit.linstor.netcom;

import java.nio.ByteBuffer;
import com.linbit.ImplementationError;

public class TcpHeaderOnlyMessage implements Message
{
    protected static final byte[] DATA = new byte[0];
    protected static final ByteBuffer EMPTY_DATA_BUFFER = ByteBuffer.wrap(DATA);

    protected final ByteBuffer headerBuffer;

    protected TcpHeaderOnlyMessage(int type)
    {
        byte[] buffer = new byte[HEADER_SIZE];
        headerBuffer = ByteBuffer.wrap(buffer);
        headerBuffer.putInt(TYPE_FIELD_OFFSET, type);
    }

    @Override
    public byte[] getData()
    {
        return DATA;
    }

    @Override
    public void setData(byte[] data)
    {
        throw new ImplementationError("Cannot set Data of TcpHeaderOnlyMessage", null);
    }

    @Override
    public int getType() throws IllegalMessageStateException
    {
        // we could also cache the constructor's parameter and return that
        // but this is more like "what would you send"
        return headerBuffer.getInt(TYPE_FIELD_OFFSET);
    }

    @Override
    public ByteBuffer getHeaderBuffer()
    {
        // creates a new ByteBuffer sharing the same byte[].
        // this has the advantage that is does not need to be rewind-ed after
        // it was completely consumed.
        return headerBuffer.asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer getDataBuffer() throws IllegalMessageStateException
    {
        return EMPTY_DATA_BUFFER;
    }
}
