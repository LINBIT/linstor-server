package com.linbit.linstor.netcom;

import com.linbit.linstor.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Abstract implementation of a message containing the plain data and header bytes
 * already wrapped in ByteBuffers for easier handling and some error checking.
 */
public class MessageData implements Message
{
    // TODO: Use ByteBuffer.allocateDirect(...) instead of ByteBuffer.wrap(...)
    protected final ByteBuffer headerBuffer;
    protected @Nullable ByteBuffer dataBuffer;

    protected final byte[] headerBytes;
    protected @Nullable byte[] dataBytes;

    private boolean forSend;

    public MessageData(boolean forSendRef)
    {
        forSend = forSendRef;

        headerBytes = new byte[HEADER_SIZE];
        headerBuffer = ByteBuffer.wrap(headerBytes);
        reset();
    }

    @Override
    public byte[] getData() throws IllegalMessageStateException
    {
        if (dataBytes == null)
        {
            throw new IllegalMessageStateException(
                "Attempt to fetch content data from a message that is not ready for processing"
            );
        }
        return dataBytes;
    }

    @Override
    public void setData(byte[] data) throws IllegalMessageStateException
    {
        dataBytes = data;
        dataBuffer = ByteBuffer.wrap(data);
        if (forSend)
        {
            headerBuffer.putInt(LENGTH_FIELD_OFFSET, data.length);
        }
    }

    @Override
    public int getType() throws IllegalMessageStateException
    {
        return headerBuffer.getInt(TYPE_FIELD_OFFSET);
    }

    protected final void reset()
    {
        Arrays.fill(headerBytes, (byte) 0);
        dataBytes = null;
        dataBuffer = null;
    }

    @Override
    public ByteBuffer getHeaderBuffer()
    {
        return headerBuffer;
    }

    @Override
    public @Nullable ByteBuffer getDataBuffer()
    {
        return dataBuffer;
    }
}
