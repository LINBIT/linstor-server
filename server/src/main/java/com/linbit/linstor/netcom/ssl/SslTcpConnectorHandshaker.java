package com.linbit.linstor.netcom.ssl;

import com.linbit.ImplementationError;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class SslTcpConnectorHandshaker
{
    @FunctionalInterface
    public interface HandshakeFinishedListener
    {
        void handshakeFinished(SocketChannel socketChannel);
    }

    private enum HandshakeState
    {
        START,
        WRAPPED, SENDING, SENT,
        FAILED
    }

    private HandshakeState state = HandshakeState.START;
    private boolean handshaking = true;

    private ByteBuffer myAppData;
    private ByteBuffer myNetData;
    private ByteBuffer peerNetData;
    private ByteBuffer peerAppData;

    private SslTcpConnectorPeer peer;
    private HandshakeFinishedListener[] finishedListeners;

    public SslTcpConnectorHandshaker(
        SslTcpConnectorPeer peerRef,
        SSLEngine sslEngine,
        HandshakeFinishedListener... finishedListenersRef
    )
    {
        SSLSession session = sslEngine.getSession();
        myAppData = ByteBuffer.allocate(session.getApplicationBufferSize() * 2);
        myNetData = ByteBuffer.allocate(session.getPacketBufferSize() * 2);
        peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize() * 2);
        peerNetData = ByteBuffer.allocate(session.getPacketBufferSize() * 2);
        session.invalidate();

        peer = peerRef;
        finishedListeners = finishedListenersRef;
    }

    void startHandshaking(SSLEngine sslEngine) throws SSLException
    {
        handshaking = true;
        sslEngine.beginHandshake();
        myAppData.clear();
        myNetData.clear();
        peerAppData.clear();
        peerNetData.clear();
        peer.setOpInterest(SelectionKey.OP_WRITE);
    }

    public boolean isHandshaking()
    {
        return handshaking;
    }

    /**
     * Performs a step of the handshaking process.
     * Does NOT perform the whole handshake, as the central networking engine is non-blocking,
     * we have to wait until our socketChannel received a ready to read / write event.
     *
     * @param socketChannel
     * @param engine
     * @return true if and only if the handshake is finished.
     * @throws IOException
     */
    @SuppressWarnings("checkstyle:descendanttoken")
    // checkstyle complains about multiple defaults inside switch (although there are nested switches)
    boolean doHandshake(
        final SocketChannel socketChannel,
        final SSLEngine engine
    )
        throws IOException
    {
        if (handshaking)
        {
            boolean retry;
            do
            {
                retry = false;
                HandshakeStatus handshakeStatus = engine.getHandshakeStatus();

                if (handshakeStatus == HandshakeStatus.NOT_HANDSHAKING)
                {
                    throw new IllegalStateException(
                        SslTcpConnectorService.class.getName() + " indicates requiring a handshake, " +
                        "but the " + engine.getClass().getName() + " instance is not in handshake mode"
                    );
                }
                do
                {
                    switch (handshakeStatus)
                    {
                        case NEED_UNWRAP_AGAIN:
                            throw new ImplementationError("This should never happen");
                        case NEED_UNWRAP:
                            {
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
                                        // after an unwrap the data is flipped again, thus we can immediately
                                        // read + flip + unwrap again
                                    }
                                    catch (SSLException sslExc)
                                    {
                                        // FIXME: Error reporting required
                                        sslExc.printStackTrace();
                                        engine.closeOutbound();
                                        handshakeStatus = engine.getHandshakeStatus();
                                    }

                                    if (result != null)
                                    {
                                        peerNetData.compact();
                                        handshakeStatus = result.getHandshakeStatus();

                                        switch (result.getStatus())
                                        {
                                            case OK:
                                                break;
                                            case BUFFER_OVERFLOW:
                                                peerAppData = enlargeApplicationBuffer(engine, peerAppData);
                                                retry = true;
                                                break;
                                            case BUFFER_UNDERFLOW:
                                                peerNetData = handleBufferUnderflow(engine, peerNetData);
                                                retry = true;
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
                                                throw new IllegalStateException("Unknown SSL state: " +
                                                    result.getStatus());
                                        }
                                        if (result.bytesConsumed() > 0 &&
                                            peerNetData.position() > 0)
                                        {
                                            retry = true;
                                        }
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
                                        throw new SSLException(
                                            "Buffer underflow while handshaking - this should never occur"
                                        );
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
                                new Thread(task).start();
                            }
                            handshakeStatus = engine.getHandshakeStatus();
                            break;
                        case FINISHED:
                            break;
                        case NOT_HANDSHAKING:
                            break;
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
                    }
                }
                while (handshakeStatus == HandshakeStatus.NEED_TASK); // do not wait if NEED_TASK

                if (state == HandshakeState.FAILED)
                {
                    peer.closeConnection();
                    throw new SSLException("Handshaking failed");
                }

                switch (handshakeStatus)
                {
                    case FINISHED:
                        handshaking = false;
                        for (HandshakeFinishedListener listener : finishedListeners)
                        {
                            listener.handshakeFinished(socketChannel);
                        }
                        socketChannel.finishConnect();
                        break;
                    case NEED_TASK:
                        // should never happen - while loop before should cover this case
                        break;
                    case NEED_UNWRAP_AGAIN:
                        throw new ImplementationError("This should never happen");
                    case NEED_UNWRAP:
                        peer.setOpInterest(SelectionKey.OP_READ);
                        break;
                    case NEED_WRAP:
                        peer.setOpInterest(SelectionKey.OP_WRITE);
                        break;
                    case NOT_HANDSHAKING:
                        peer.closeConnection();
                        throw new SSLException("Not handshaking");
                    default:
                        break;
                }
            }
            while (retry);
        }
        return !handshaking;
    }

    protected ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer)
    {
        return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
    }

    protected ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer)
    {
        return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
    }

    protected ByteBuffer enlargeBuffer(final ByteBuffer buffer, int sessionProposedCapacity)
    {
        ByteBuffer newBuffer;
        if (sessionProposedCapacity > buffer.capacity())
        {
            newBuffer = ByteBuffer.allocate(sessionProposedCapacity);
        }
        else
        {
            newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
        }
        return newBuffer;
    }

    protected ByteBuffer handleBufferUnderflow(final SSLEngine engine, final ByteBuffer buffer)
    {
        ByteBuffer adjustedBuffer = buffer;
        if (engine.getSession().getPacketBufferSize() >= buffer.limit())
        {
            ByteBuffer replaceBuffer = enlargePacketBuffer(engine, buffer);
            buffer.flip();
            replaceBuffer.put(buffer);
            adjustedBuffer = replaceBuffer;
        }
        return adjustedBuffer;
    }
}
