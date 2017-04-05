package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage;
import com.linbit.drbdmanage.netcom.TcpConnectorPeer;
import com.linbit.drbdmanage.security.AccessContext;

public class SslTcpConnectorPeer extends TcpConnectorPeer
{
    private enum HandshakeState
    {
        START,
        WRAPPED, SENDING, SENT,
        FAILED
    }

    private SSLEngine sslEngine;

    private ByteBuffer myAppData;
    private ByteBuffer myNetData;
    private ByteBuffer peerAppData;
    private ByteBuffer peerNetData;

    private boolean needsHandshake = true;
    private HandshakeState state = HandshakeState.START;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public SslTcpConnectorPeer(
        final String peerId,
        final SslTcpConnectorService sslConnectorService,
        final SelectionKey connKey,
        final AccessContext peerAccCtx,
        final SSLContext sslCtx,
        final InetSocketAddress address
    ) throws SSLException
    {
        super(peerId, sslConnectorService, connKey, peerAccCtx);

        if (address != null)
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
            sslEngine.setNeedClientAuth(false);
            sslEngine.setUseClientMode(false);

            SSLSession session = sslEngine.getSession();
            myAppData = ByteBuffer.allocate(session.getApplicationBufferSize() * 2);
            myNetData = ByteBuffer.allocate(session.getPacketBufferSize() * 2);
            peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize() * 2);
            peerNetData = ByteBuffer.allocate(session.getPacketBufferSize() * 2);
            session.invalidate();
        }
        sslEngine.beginHandshake();

        SslTcpConnectorHandshakeMessage handshakeMessage = new SslTcpConnectorHandshakeMessage(false, sslEngine, this);
        // Overwrite msgIn, which was initialized by the superclass with an empty message,
        // with a reference to a message that has an initialized SSLEngine and that will be
        // used for the handshake
        msgIn = handshakeMessage;

        try
        {
            sendMessage(handshakeMessage);
        }
        catch (IllegalMessageStateException illegalStateExc)
        {
            throw new ImplementationError("Dummy SSL handshake message broken", illegalStateExc);
        }
    }

    @Override
    protected TcpConnectorMessage createMessage(boolean forSend)
    {
        try
        {
            // FIXME: Null is definitely wrong here
            return new SslTcpConnectorMessage(forSend, sslEngine, this);
        }
        catch (SSLException sslExc)
        {
            throw new RuntimeException(sslExc);
        }
    }

    // Only SSL-Clients should call this method
    public void encryptConnection(final SocketChannel channel) throws IOException
    {
        sslEngine.beginHandshake();
        needsHandshake = true;
        doHandshake(channel, sslEngine);
    }

    /**
     * Performs a step of the handshaking process.
     * Does NOT perform the whole handshake, as the central networking engine is non-blocking, thus
     * we have to wait until our socketChannel received a ready to read / write event.
     *
     * @param socketChannel
     * @param engine
     * @return true if and only if the handshake is finished.
     * @throws IOException
     */
    boolean doHandshake(
        final SocketChannel socketChannel,
        final SSLEngine engine
    )
        throws IOException
    {
        if (needsHandshake)
        {
            HandshakeStatus handshakeStatus = engine.getHandshakeStatus();

            if (handshakeStatus == HandshakeStatus.NOT_HANDSHAKING)
            {
                throw new ImplementationError(
                    SslTcpConnectorService.class.getName() + " indicates requiring a handshake, " +
                    "but the " + engine.getClass().getName() + " instance is not in handshake mode",
                    new IllegalStateException()
                );
            }
            switch (handshakeStatus)
            {
                case NEED_UNWRAP:
                    int read = socketChannel.read(peerNetData);
                    if (read < 0)
                    {
                        if (!engine.isInboundDone() || !engine.isOutboundDone())
                        {
                            engine.closeInbound();
                            engine.closeOutbound();
                            handshakeStatus = engine.getHandshakeStatus();
                        }
                    }
                    else
                    {
                        peerNetData.flip();
                        SSLEngineResult result = null;
                        try
                        {
                            result = engine.unwrap(peerNetData, peerAppData);
                            peerNetData.compact();
                            handshakeStatus = result.getHandshakeStatus();
                        }
                        catch (SSLException sslExc)
                        {
                            // FIXME: Error reporting required
                            sslExc.printStackTrace();
                            engine.closeOutbound();
                            handshakeStatus = engine.getHandshakeStatus();
                            result = null;
                        }

                        if (result != null)
                        {
                            switch (result.getStatus())
                            {
                                case OK:
                                    break;
                                case BUFFER_OVERFLOW:
                                    peerAppData = enlargeApplicationBuffer(engine, peerAppData);
                                    break;
                                case BUFFER_UNDERFLOW:
                                    peerNetData = handleBufferUnderflow(engine, peerNetData);
                                    break;
                                case CLOSED:
                                    if (engine.isOutboundDone())
                                    {
                                        throw new SSLException("Handshaking failed");
                                        // handshakeSuccess = false;
                                        // handshakeStatus = HandshakeStatus.NOT_HANDSHAKING;
                                    }
                                    else
                                    {
                                        engine.closeOutbound();
                                        handshakeStatus = engine.getHandshakeStatus();
                                    }
                                    break;
                                default:
                                    throw new IllegalStateException("Unknown SSL state: " + result.getStatus());
                            }
                        }
                    }
                    break;
                case NEED_WRAP:
                    myNetData.clear();
                    SSLEngineResult result;
                    try
                    {
                        result = engine.wrap(myAppData, myNetData);
                        handshakeStatus = result.getHandshakeStatus();
                        state = HandshakeState.WRAPPED;
                    }
                    catch (SSLException sslExc)
                    {
                        // FIXME: Error reporting required
                        sslExc.printStackTrace();
                        engine.closeOutbound();
                        handshakeStatus = engine.getHandshakeStatus();
                        result = null;
                        state = HandshakeState.FAILED;
                    }

                    if (result != null)
                    {
                        switch (result.getStatus())
                        {
                            case OK:
                                if (state == HandshakeState.WRAPPED)
                                {
                                    myNetData.flip();
                                    state = HandshakeState.SENDING;
                                }
                                socketChannel.write(myNetData);
                                if (!myNetData.hasRemaining())
                                {
                                    state = HandshakeState.SENT;
                                }
                                break;
                            case BUFFER_OVERFLOW:
                                myNetData = enlargePacketBuffer(engine, myNetData);
                                break;
                            case BUFFER_UNDERFLOW:
                                throw new SSLException("Buffer underflow while handshaking - this should never occur");
                            case CLOSED:
                                try
                                {
                                    if (state == HandshakeState.WRAPPED)
                                    {
                                        myNetData.flip();
                                        state = HandshakeState.SENDING;
                                    }
                                    socketChannel.write(myNetData);
                                    if (!myNetData.hasRemaining())
                                    {
                                        state = HandshakeState.SENT;
                                    }
                                    peerNetData.clear();
                                }
                                catch (Exception exc)
                                {
                                    // FIXME: Should probably catch SSLException, IOException, others?
                                    // Socket failed to send CLOSE message
                                    handshakeStatus = engine.getHandshakeStatus();
                                }
                                break;
                            default:
                                throw new IllegalStateException("Unknown SSL state: " + result.getStatus());
                        }
                    }
                    break;
                case NEED_TASK:
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null)
                    {
                        executor.execute(task); //TODO run this in a workerpool or something more central
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                case FINISHED:
                    break;
                case NOT_HANDSHAKING:
                    break;
                default:
                    throw new IllegalStateException("Invalid SSL status: " +handshakeStatus);
            }
            if (handshakeStatus == HandshakeStatus.FINISHED)
            {
                needsHandshake = false;
                msgIn = createMessage(false); // prepare the next message
            }
            else
            {
                if (state == HandshakeState.FAILED)
                {
                    throw new SSLException("Handshaking failed");
                }
            }
        }
        return !needsHandshake;
    }

    protected ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer)
    {
        return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
    }

    protected ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer)
    {
        return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
    }

    protected ByteBuffer enlargeBuffer(ByteBuffer buffer, int sessionProposedCapacity)
    {
        if (sessionProposedCapacity > buffer.capacity())
        {
            buffer = ByteBuffer.allocate(sessionProposedCapacity);
        }
        else
        {
            buffer = ByteBuffer.allocate(buffer.capacity() * 2);
        }
        return buffer;
    }

    protected ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer)
    {
        if (engine.getSession().getPacketBufferSize() >= buffer.limit())
        {
            ByteBuffer replaceBuffer = enlargePacketBuffer(engine, buffer);
            buffer.flip();
            replaceBuffer.put(buffer);
            buffer = replaceBuffer;
        }
        return buffer;
    }
}
