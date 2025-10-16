package com.linbit.linstor.netcom;

import java.nio.ByteBuffer;

/**
 * Message interface for sending and receiving data
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Message
{
    // Maximum data size of a message
    // 64 MiB == 0x4000000
    int DEFAULT_MAX_DATA_SIZE = 0x4000000;

    // 16 bytes (128 bits) message header
    int HEADER_SIZE = 16;

    // Header field for message type: 4 bytes (32 bits)
    int TYPE_FIELD_SIZE = 4;
    int TYPE_FIELD_OFFSET = 0;

    // Header field for message length: 4 bytes (32 bits)
    int LENGTH_FIELD_SIZE = 4;
    int LENGTH_FIELD_OFFSET = 4;

    byte[] getData() throws IllegalMessageStateException;

    void setData(byte[] data) throws IllegalMessageStateException;

    int getType() throws IllegalMessageStateException;

    ByteBuffer getHeaderBuffer();

    ByteBuffer getDataBuffer() throws IllegalMessageStateException;
}
