package com.linbit.drbdmanage.netcom;

import com.linbit.ImplementationError;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpConnectorMessage implements Message
{
    // 16 bytes (128 bits) message header
    protected static final int HEADER_SIZE = 16;

    // Header field for message type: 4 bytes (32 bits)
    protected static final int TYPE_FIELD_SIZE = 4;
    protected static final int TYPE_FIELD_OFFSET = 0;

    // Header field for message length: 4 bytes (32 bits)
    protected static final int LENGTH_FIELD_SIZE = 4;
    protected static final int LENGTH_FIELD_OFFSET = 4;

    // Maximum data size of a message
    // 16 MiB == 0x1000000
    public static final int DEFAULT_MAX_DATA_SIZE = 0x1000000;

    private final byte[] headerBytes;
    private byte[] dataBytes;

    // TODO: Use ByteBuffer.allocateDirect(...) instead of ByteBuffer.wrap(...)
    private final ByteBuffer headerBuffer;
    private ByteBuffer dataBuffer;

    protected enum Phase
    {
        PREPARE,
        HEADER,
        DATA,
        PROCESS;

        public Phase getNextPhase()
        {
            Phase nextPhase;
            switch (this)
            {
                case HEADER:
                    nextPhase = DATA;
                    break;
                case DATA:
                    // fall-through
                default:
                    nextPhase = PROCESS;
                    break;
            }
            return nextPhase;
        }
    };

    protected enum ReadState
    {
        UNFINISHED,
        FINISHED,
        END_OF_STREAM
    };

    protected enum WriteState
    {
        UNFINISHED,
        FINISHED
    };

    protected Phase currentPhase;

    protected TcpConnectorMessage(boolean forSend)
    {
        headerBytes = new byte[HEADER_SIZE];
        headerBuffer = ByteBuffer.wrap(headerBytes);
        reset(forSend);
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
        if (currentPhase != Phase.PREPARE)
        {
            throw new IllegalMessageStateException(
                "Attempt to fetch content data from a message that is not ready for processing"
            );
        }
        dataBytes = data;
        dataBuffer = ByteBuffer.wrap(dataBytes);
        headerBuffer.putInt(LENGTH_FIELD_OFFSET, data.length);
        currentPhase = Phase.HEADER;
    }

    protected ReadState read(SocketChannel inChannel)
        throws IllegalMessageStateException, IOException
    {
        ReadState state = ReadState.UNFINISHED;
        switch (currentPhase)
        {
            case PREPARE:
                throw new IllegalMessageStateException(
                    "Attempt to read a message that is in prepare mode for sending"
                );
            case HEADER:
                {
                    int readCount = read(inChannel, headerBuffer);
                    if (readCount > -1)
                    {
                        if (!headerBuffer.hasRemaining())
                        {
                            // All header data has been received
                            // Prepare reading the message
                            int dataSize = headerBuffer.getInt(LENGTH_FIELD_OFFSET);
                            if (dataSize < 0)
                            {
                                dataSize = 0;
                            }
                            if (dataSize > DEFAULT_MAX_DATA_SIZE)
                            {
                                dataSize = DEFAULT_MAX_DATA_SIZE;
                            }
                            dataBytes = new byte[dataSize];
                            dataBuffer = ByteBuffer.wrap(dataBytes);
                            currentPhase = currentPhase.getNextPhase();

                            readCount = read(inChannel, dataBuffer);
                            if (readCount <= -1)
                            {
                                // Peer has closed the stream
                                state = ReadState.END_OF_STREAM;
                            }
                            if (!dataBuffer.hasRemaining())
                            {
                                // All message data has been received
                                currentPhase = currentPhase.getNextPhase();
                                state = ReadState.FINISHED;
                            }
                        }
                    }
                    else
                    {
                        // Peer has closed the stream
                        state = ReadState.END_OF_STREAM;
                    }
                }
                break;
            case DATA:
                {
                    int readCount = read(inChannel, dataBuffer);
                    if (readCount <= -1)
                    {
                        // Peer has closed the stream
                        state = ReadState.END_OF_STREAM;
                    }
                    if (!dataBuffer.hasRemaining())
                    {
                        // All message data has been received
                        currentPhase = currentPhase.getNextPhase();
                        state = ReadState.FINISHED;
                    }
                }
                break;
            case PROCESS:
                throw new IllegalMessageStateException(
                    "Attempt to read a message that is in process mode"
                );
            default:
                throw new ImplementationError(
                    String.format(
                        "Missing case label for enum member '%s'",
                        currentPhase.name()
                    ),
                    null
                );
        }
        return state;
    }

    protected WriteState write(SocketChannel outChannel)
        throws IllegalMessageStateException, IOException
    {
        WriteState state = WriteState.UNFINISHED;
        boolean flushed;
        switch (currentPhase)
        {
            case PREPARE:
                throw new IllegalMessageStateException(
                    "Attempt to write a message that is in prepare mode for sending"
                );
            case HEADER:
                flushed = write(outChannel, headerBuffer);
                if (flushed)
                {
                    flushed = write(outChannel, dataBuffer);
                    if (!flushed)
                    {
                        // Continue the next invocation of read() with the DATA phase
                        currentPhase = currentPhase.getNextPhase();
                    }
                    else
                    {
                        // Finished sending the message
                        state = WriteState.FINISHED;
                    }
                }
                break;
            case DATA:
                flushed = write(outChannel, dataBuffer);
                if (flushed)
                {
                    // Finished sending the message
                    state = WriteState.FINISHED;
                }
            case PROCESS:
                throw new IllegalMessageStateException(
                    "Attempt to write a message that is in process mode"
                );
            default:
                throw new ImplementationError(
                    String.format(
                        "Missing case label for enum member '%s'",
                        currentPhase.name()
                    ),
                    null
                );
        }
        return state;
    }
    /**
     * Needed so that SSLConnectorMessage can override this method
     *
     * @param channel
     * @param buffer
     * @return the amount of bytes written into the buffer
     * @throws IOException
     */
    protected int read(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        return channel.read(buffer);
    }

    /**
     * Needed so that SSLConnectorMessage can override this method
     *
     * @param channel
     * @param buffer
     * @return true if the buffer was completely sent. false otherwise.
     * @throws IOException
     */
    protected boolean write(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        channel.write(buffer);
        return !buffer.hasRemaining();
    }

    final void reset(boolean forSend)
    {
        Arrays.fill(headerBytes, (byte) 0);
        dataBytes = null;
        dataBuffer = null;
        if (forSend)
        {
            currentPhase = Phase.PREPARE;
        }
        else
        {
            currentPhase = Phase.HEADER;
        }
    }
}
