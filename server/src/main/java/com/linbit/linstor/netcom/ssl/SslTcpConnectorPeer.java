package com.linbit.linstor.netcom.ssl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.TcpConnectorPeer;
import com.linbit.linstor.security.AccessContext;

public class SslTcpConnectorPeer extends TcpConnectorPeer
{
    private final boolean clientMode;

    private SSLEngine sslEngine;
    private SSLContext sslCtx;
    private ByteBuffer encryptedReadBuffer;
    private ByteBuffer encryptedWriteBuffer;
    private ByteBuffer decryptedReadBuffer;

    private InetSocketAddress address;
    private SslTcpConnectorHandshaker handshaker;

    public SslTcpConnectorPeer(
        final ErrorReporter errorReporter,
        final CommonSerializer commonSerializer,
        final String peerId,
        final SslTcpConnectorService sslConnectorService,
        final SelectionKey connKey,
        final AccessContext peerAccCtx,
        final SSLContext sslCtxRef,
        final InetSocketAddress peerAddress,
        final Node node
    )
        throws SSLException
    {
        super(errorReporter, commonSerializer, peerId, sslConnectorService, connKey, peerAccCtx, node);
        sslCtx = sslCtxRef;
        address = peerAddress;

        clientMode = address != null;

        if (clientMode)
        {
            // Client mode
            String host = address.getAddress().getHostAddress();
            int port = address.getPort();
            sslEngine = sslCtx.createSSLEngine(host, port);
            sslEngine.setUseClientMode(true);
        }
        else
        {
            // Server mode
            sslEngine = sslCtx.createSSLEngine();
            sslEngine.setNeedClientAuth(true);
            sslEngine.setUseClientMode(false);
        }

        handshaker = new SslTcpConnectorHandshaker(
            this,
            sslEngine,
            socketChannel ->
            {
                nextInMessage(); // prepare the next messages
                if (msgOut != null)
                {
                    setOpInterest(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
                }
                else
                {
                    setOpInterest(SelectionKey.OP_READ);
                }
                pongReceived(); // fix the pongReceived-pingSent timestamp-delta
            }
        );

        sslEngine.closeInbound();
        sslEngine.closeOutbound();

        // FIXME: using byte[] only for debugging purposses...
//        encryptedReadBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
//        encryptedWriteBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
//        decryptedReadBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getApplicationBufferSize());
        encryptedReadBuffer = ByteBuffer.wrap(new byte[sslEngine.getSession().getPacketBufferSize()]);
        decryptedReadBuffer = ByteBuffer.wrap(new byte[sslEngine.getSession().getApplicationBufferSize()]);
        encryptedWriteBuffer = ByteBuffer.wrap(new byte[sslEngine.getSession().getPacketBufferSize()]);
        encryptedReadBuffer.limit(0);
        decryptedReadBuffer.limit(0);
        encryptedWriteBuffer.limit(0);
    }

    @Override
    public boolean isConnected()
    {
        return super.isConnected() && !handshaker.isHandshaking();
    }

    @Override
    public void connectionEstablished() throws SSLException
    {
        if (clientMode)
        {
            // Client mode
            String host = address.getAddress().getHostAddress();
            int port = address.getPort();
            sslEngine = sslCtx.createSSLEngine(host, port);
            sslEngine.setUseClientMode(true);
        }
        else
        {
            // Server mode
            sslEngine = sslCtx.createSSLEngine();
            sslEngine.setNeedClientAuth(true);
            sslEngine.setUseClientMode(false);
        }

        handshaker.startHandshaking(sslEngine);

        super.connectionEstablished();
        if (clientMode)
        {
            setOpInterest(SelectionKey.OP_WRITE); // client starts handshake
        }
        else
        {
            setOpInterest(SelectionKey.OP_READ);
        }
    }

    // Only SSL-Clients should call this method
    public void encryptConnection() throws IOException
    {
        handshaker.startHandshaking(sslEngine);
    }

    /*
     * The {@link TcpConnectorPeer} consumes only as much bytes as the current message needs. That
     * means that the remaining bytes will stay in the {@link SocketChannel}, thus the
     * {@code TcpConnectorPeer} will be called again and again as long as there are bytes in the
     * {@code SocketChannel}.
     *
     * The {@link SslTcpConnectorPeer} cannot leave bytes in the {@code SocketChannel} as it simply
     * does not know how much encrypted bytes it needs for the current message. That means,
     * it has to read as much bytes as possible in order to be able to fill the current message.
     * However, doing so, it is very likely that the {@code SslTcpConnectorPeer} will read more
     * encrypted bytes as needed, maybe even read enough bytes for multiple complete messages.
     * Additionally it is also likely that all bytes are consumed from the {@code SocketChannel},
     * which means that this method has to be able to create multiple messages in one invocation.
     */
    @Override
    public ReadState read(SocketChannel channel) throws IOException, IllegalMessageStateException
    {
        ReadState retState = ReadState.UNFINISHED;
        if (handshaker.isHandshaking())
        {
            handshaker.doHandshake(channel, sslEngine);
        }
        else
        {
            /*
             * copy the not yet consumed bytes to the beginning of the buffer
             * after compact:
             * pos = index of first unoccupied byte
             * limit = capacity
             *
             * if there are no bytes left (pos == limit), .compact() behaves the same as .clear()
             */
            encryptedReadBuffer.compact();
            int encryptedRead = channel.read(encryptedReadBuffer);
            encryptedReadBuffer.flip(); // make buffer ready to read
            if (encryptedRead > -1)
            {
                boolean hasDataToDecrypt = true; // we will try once at least :)
                while (hasDataToDecrypt)
                {
                    ReadState msgState = ReadState.UNFINISHED;
                    switch (currentReadPhase)
                    {
                        case HEADER:
                            {
                                ByteBuffer decryptedHeaderBuffer = msgIn.getHeaderBuffer();
                                unwrapInto(decryptedHeaderBuffer);

                                if (!decryptedHeaderBuffer.hasRemaining())
                                {
                                    // All header data has been received
                                    // Prepare reading the message
                                    initDataByteBuffer(decryptedHeaderBuffer);
                                    msgState = readData(msgIn.getDataBuffer(), msgState);
                                }
                            }
                            break;
                        case DATA:
                            msgState = readData(msgIn.getDataBuffer(), msgState);
                            break;
                        default:
                            throw new ImplementationError(
                                String.format(
                                    "Missing case label for enum member '%s'",
                                    currentReadPhase.name()
                                ),
                                null
                            );

                    }
                    if (msgState == ReadState.FINISHED)
                    {
                        addToQueue(msgIn);
                        msgIn = createMessage(false);
                        retState = ReadState.FINISHED;
                    }
                    else
                    {
                        hasDataToDecrypt = false;
                    }
                }
            }
            else
            {
                // Peer has closed the stream
                retState = ReadState.END_OF_STREAM;
            }
        }

        return retState;
    }

    private void unwrapInto(ByteBuffer decryptedBuffer) throws SSLException
    {
        decryptedReadBuffer.compact();
        SSLEngineResult sslResult = sslEngine.unwrap(encryptedReadBuffer, decryptedReadBuffer);
        decryptedReadBuffer.flip();
        if (sslResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
        {
            throw new ImplementationError("Target decrypted buffer is too small!");
        }
        copyDecryptedDataInto(decryptedBuffer);
    }

    private void copyDecryptedDataInto(ByteBuffer targetBuffer)
    {
        final int oldDecryptLimit = decryptedReadBuffer.limit();
        final int oldDecryptPos = decryptedReadBuffer.position();
        if (decryptedReadBuffer.hasArray() && targetBuffer.hasArray())
        {
            final int origBufferLimit = targetBuffer.limit();
            final int origBufferPos = targetBuffer.position();

            // get the backing arrays
            final byte[] decryptArr = decryptedReadBuffer.array();
            final byte[] bufferArr = targetBuffer.array();

            final int copyLen = Math.min(origBufferLimit - origBufferPos, oldDecryptLimit - oldDecryptPos);

            // copy
            System.arraycopy(decryptArr, oldDecryptPos, bufferArr, origBufferPos, copyLen);

            // mark the copied bytes as consumed
            decryptedReadBuffer.position(oldDecryptPos + copyLen);
            targetBuffer.position(origBufferPos + copyLen);
        }
        else
        {
            if (decryptedReadBuffer.remaining() > targetBuffer.remaining())
            {
                // adjust the limit, such that the we do no try to copy too many bytes (would lead to
                // BufferOverflowException thrown by the .put method
                decryptedReadBuffer.limit(oldDecryptPos + targetBuffer.remaining());
            }
            // copy the bytes (implicitly marking the bytes as consumed)
            targetBuffer.put(decryptedReadBuffer);
            // restore the original limit
            decryptedReadBuffer.limit(oldDecryptLimit);
            // read = decryptedReadBuffer.position() - oldDecryptPos;
        }
    }

    private ReadState readData(ByteBuffer decryptedBuffer, ReadState stateRef) throws SSLException
    {
        ReadState state = stateRef;
        unwrapInto(decryptedBuffer);
        if (!decryptedBuffer.hasRemaining())
        {
            currentReadPhase = currentReadPhase.getNextPhase();
            state = ReadState.FINISHED;
        }
        return state;
    }

    /*
     * Unlike the {@link #read(SocketChannel)} method, this method can encrypt and send only one message
     * with each invocation. This is possible as the {@link TcpConnectorPeer} will let the
     * {@link SelectionKey#OP_WRITE} enabled as long as it has outgoing messages.
     */
    @Override
    public WriteState write(SocketChannel outChannel) throws IllegalMessageStateException, IOException
    {
        WriteState state = WriteState.UNFINISHED;
        if (handshaker.isHandshaking())
        {
            handshaker.doHandshake(outChannel, sslEngine);
        }
        else
        {
            /*
             * copy the not yet consumed bytes to the beginning of the buffer
             * after compact:
             * pos = index of first unoccupied byte
             * limit = capacity
             *
             * if there are no bytes left (pos == limit), .compact() behaves the same as .clear()
             */
            encryptedWriteBuffer.compact();
            // first try to encrypt the new message
            switch (currentWritePhase)
            {
                case HEADER:
                    {
                        ByteBuffer decryptedHeaderBuffer = msgOut.getHeaderBuffer();
                        sslEngine.wrap(decryptedHeaderBuffer, encryptedWriteBuffer);

                        if (!decryptedHeaderBuffer.hasRemaining())
                        {
                            currentWritePhase = currentWritePhase.getNextPhase();
                            state = wrapData(state);
                        }
                    }
                    break;
                case DATA:
                    state = wrapData(state);
                    break;
                default:
                    throw new ImplementationError(
                        String.format(
                            "Missing case label for enum member '%s'",
                            currentWritePhase.name()
                        ),
                        null
                    );
            }
            // we just (partially?) encrypted data into the encrytpedWriteBuffer
            // try to send it
            encryptedWriteBuffer.flip(); // make ready to send
            outChannel.write(encryptedWriteBuffer);

            if (state == WriteState.FINISHED)
            {
                nextOutMessage();
                currentWritePhase = Phase.HEADER; // prepare for next write
            }
        }
        return state;
    }

    private WriteState wrapData(WriteState stateRef)
        throws SSLException, IllegalMessageStateException
    {
        WriteState state = stateRef;
        ByteBuffer decryptedDataBuffer = msgOut.getDataBuffer();

        sslEngine.wrap(decryptedDataBuffer, encryptedWriteBuffer);
        if (!decryptedDataBuffer.hasRemaining())
        {
            currentWritePhase = currentWritePhase.getNextPhase();
            state = WriteState.FINISHED;
        }

        return state;
    }

    // overriding and only calling super so that this method is exposed to the current package
    @Override
    protected void setOpInterest(int op)
    {
        super.setOpInterest(op);
    }
}
