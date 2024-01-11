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
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.TcpConnectorPeer;
import com.linbit.linstor.security.AccessContext;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLSession;


/**
 * Implements SSL/TLS encrypted connections for LINSTOR peers.
 */
public class SslTcpConnectorPeer extends TcpConnectorPeer
{
    /**
     * clientMode: true if connecting, false if accepting connections
     */
    private final boolean           clientMode;

    /**
     * True if SSL is ready to transfer data
     * (after the initial handshake on a new network connection has been completed)
     */
    private boolean                 sslReady;

    /**
     * True if network I/O is required before SSL handling can continue
     */
    private boolean                 ioRequest;

    private SSLEngine               sslEngine;
    private SSLContext              sslCtx;
    private InetSocketAddress       address;

    /**
     * Buffer for received SSL encrypted data
     */
    private ByteBuffer              encryptedReadBuffer;

    /**
     * Buffer for SSL encrypted data to send
     */
    private ByteBuffer              encryptedWriteBuffer;

    /**
     * Buffer for received data that has been decrypted
     */
    private ByteBuffer              plainReadBuffer;

    /**
     * Dummy buffer for handshake operations without application data
     */
    private ByteBuffer              dummyBuffer;

    /**
     * SSL asynchronous task management - synchronization lock
     */
    private Lock                    sslTaskLock;

    /**
     * SSL asynchronous task management - number of pending tasks
     */
    private int                     activeTaskCount;

    /**
     * SSL asynchronous task management - cancelation flag
     */
    private boolean                 sslTasksCanceled;

    /**
     * State after I/O read (network receive) operations
     */
    private ReadState               rdState;

    /**
     * Creates a new instance.
     * If <code>peerAddress</code> is non-null, a client-side peer is created, and its connection will be outbound
     * to a server; otherwise, a server-side peer is created, and its connection will be created by accepting
     * an inbound connection request from another peer.
     *
     * @param errorReporter ErrorReporter to be used for logging
     * @param commonSerializer Serializer for various protobuf messages
     * @param peerId LINSTOR netcom ID for this connection (usually identified by the peer's address)
     * @param sslConnectorService LINSTOR netcom connector for this connection
     * @param connKey SelectionKey associated with this connection, for the nonblocking I/O Selector
     * @param peerAccCtx AccessContext associated with the peer (the peer's security credentials)
     * @param sslCtxRef SSLContext for this SSL connection (SSL settings for the SSLEngine)
     * @param peerAddress Address to connect to; null when accepting an inbound connection
     * @param node LINSTOR Node object associated with this connection (e.g., the connected LINSTOR satellite)
     */
    public SslTcpConnectorPeer(
        @Nonnull final ErrorReporter            errorReporter,
        @Nonnull final CommonSerializer         commonSerializer,
        @Nonnull final String                   peerId,
        @Nonnull final SslTcpConnectorService   sslConnectorService,
        @Nonnull final SelectionKey             connKey,
        @Nonnull final AccessContext            peerAccCtx,
        @Nonnull final SSLContext               sslCtxRef,
        @Nullable final InetSocketAddress       peerAddress,
        @Nonnull final Node                     node
    )
    {
        super(errorReporter, commonSerializer, peerId, sslConnectorService, connKey, peerAccCtx, node);
        address     = peerAddress;
        clientMode  = peerAddress != null;
        sslReady    = false;
        ioRequest   = false;
        sslEngine   = null;
        sslCtx      = sslCtxRef;

        encryptedReadBuffer     = null;
        encryptedWriteBuffer    = null;
        plainReadBuffer         = null;
        dummyBuffer             = null;

        sslTaskLock         = new ReentrantLock();
        activeTaskCount     = 0;
        sslTasksCanceled    = false;

        rdState             = ReadState.UNFINISHED;
    }

    /**
     * Indicates whether this peer connection is considered to be online.
     * The peer connection is considered to be online if there is an active network connection, the SSL handshake
     * was completed successfully and the peer has been authenticated.
     *
     * @return true if this peer connection is considered to be online
     */
    @Override
    public boolean isOnline()
    {
        return super.isOnline() && sslReady;
    }

    /**
     * Called whenever an inbound connection is accepted or an outbound connection attempt completes.
     * This method initializes the peer object. Calling the <code>read</code> method or <code>write</code> method
     * before this method has completed initialization of the peer object is an implementation error.
     *
     * @throws SSLException if the JCL SSL implementation generates the exception
     */
    @Override
    public void connectionEstablished()
        throws SSLException
    {
        sslReady = false;

        if (clientMode)
        { // Client mode
            final String host = address.getAddress().getHostAddress();
            final int port = address.getPort();
            sslEngine = sslCtx.createSSLEngine(host, port);
        }
        else
        { // Server mode
            sslEngine = sslCtx.createSSLEngine();
            sslEngine.setNeedClientAuth(true);
        }
        sslEngine.setUseClientMode(clientMode);

        final SSLSession session = sslEngine.getSession();
        final int pktBfrSize = session.getPacketBufferSize();
        final int appBfrSize = session.getApplicationBufferSize();

        encryptedReadBuffer = ByteBuffer.allocate(pktBfrSize);
        encryptedReadBuffer.limit(0);

        encryptedWriteBuffer = ByteBuffer.allocate(pktBfrSize);
        encryptedWriteBuffer.limit(0);

        plainReadBuffer = ByteBuffer.allocate(appBfrSize);
        plainReadBuffer.limit(0);

        dummyBuffer = ByteBuffer.allocate(0);

        sslEngine.beginHandshake();

        setInterestOps(clientMode ? SelectionKey.OP_WRITE : SelectionKey.OP_READ);

        super.connectionEstablished();
    }

    /**
     * Reads (receives) inbound data from the network and passes it through the SSL engine.
     * SSL encrypted data received from the network is read into a buffer and is then passed through the SSL engine,
     * either to advance an SSL handshake (including renegotiation), or to decrypt the data that was received and
     * transform the unencrypted data into LINSTOR messages.
     * The method call may produce unencrypted data, but there is no guarantee that it will, even if
     * encrypted data is available, because the SSL engine might need to receive more encrypted data first, or
     * may need to resize the buffers involved in the operation.
     * If enough unencrypted data is available, the method call will produce and queue one or multiple
     * inbound LINSTOR messages.
     *
     * Calling the method when there is no data ready to be read causes a zero-length read operation, which is
     * indistinguishable from the end-of-stream condition, and therefore, doing that is an implementation error.
     *
     * The method call may change the set of I/O operations that the peer object is waiting for, or may produce one
     * or multiple asynchronously running SSL engine tasks in separate threads and suspend all I/O operations. Upon
     * completion of those SSL engine tasks, the peer object will be placed into a connector service queue, and the
     * connector service will be notified to continue SSL processing for this peer object.
     *
     * @param inChannel The network channel associated with this peer object
     * @return ReadState object describing the result of the read operation
     * @throws IllegalMessageStateException If the LINSTOR message is in illegal state for writing data into it;
     *         not supposed to happen, indicates an implementation error
     * @throws IOException If the network read operation fails, or if SSL processing fails
     */
    @Override
    public ReadState read(@Nonnull final SocketChannel inChannel)
        throws IllegalMessageStateException, IOException
    {
        rdState = ReadState.UNFINISHED;

        encryptedReadBuffer.compact();
        final int readSize = inChannel.read(encryptedReadBuffer);

        if (readSize >= 0)
        {
            encryptedReadBuffer.flip();

            SSLEngineResult.Status trafficStatus = null;
            ioRequest = false;
            while (!ioRequest)
            {
                SSLEngineResult.HandshakeStatus hsStatus = sslEngine.getHandshakeStatus();
                if (hsStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
                { // SSL decrypt application data
                    sslReady = true;
                    trafficStatus = sslInbound();
                }
                else
                {
                    trafficStatus = sslHandshake(hsStatus, false);
                }
            }

            if (trafficStatus == SSLEngineResult.Status.CLOSED)
            {
                sslEngine.closeInbound();
                closeConnection(true);
                rdState = ReadState.END_OF_STREAM;
            }
        }
        else
        {
            closeConnection(true);
            rdState = ReadState.END_OF_STREAM;
        }
        return rdState;
    }

    /**
     * Passes encrypted data through the SSL engine or advances the SSL handshake.
     * Buffered encrypted data is passsed through the SSL engine, thereby potentially producing unencrypted data
     * that is transformed into inbound LINSTOR messages. If enough data is available, this method call will
     * produce and queue one or multiple inbound LINSTOR messages. If the SSL engine reports that not enough data
     * is available, the method call will change the set of I/O operations that the peer object is waiting, so
     * that the connector service will invoke this peer object's <code>read</code> method when more data is
     * available for reading. If required by the SSL engine, the method call may also resize the buffers involved
     * in the operation.
     *
     * @return result of the SSLEngine unwrap operation
     * @throws SSLException if the JCL SSL engine generates the exception
     * @throws IllegalMessageStateException  If the LINSTOR message is in illegal state for writing data into it;
     *         not supposed to happen, indicates an implementation error
     */
    private SSLEngineResult.Status sslInbound()
        throws SSLException, IllegalMessageStateException
    {
        plainReadBuffer.compact();
        SSLEngineResult sslStatus = sslEngine.unwrap(encryptedReadBuffer, plainReadBuffer);
        SSLEngineResult.Status trafficStatus = sslStatus.getStatus();
        if (trafficStatus == SSLEngineResult.Status.BUFFER_OVERFLOW)
        {
            if (plainReadBuffer.position() == 0)
            { // Buffer too small
                adjustPlainReadBuffer();
            }
        }
        else
        if (trafficStatus == SSLEngineResult.Status.BUFFER_UNDERFLOW)
        {
            if (encryptedReadBuffer.remaining() == encryptedReadBuffer.capacity())
            { // Buffer too small
                adjustEncryptedReadBuffer();
            }
            // Read more data into the buffer
            enableInterestOps(SelectionKey.OP_READ);
            ioRequest = true;
        }

        extractMessages();

        return trafficStatus;
    }

    /**
     * Passes outbound data through the SSL engine and writes (sends) it to the network.
     * Unencrypted data contained in LINSTOR messages is passed through the SSL engine and is thereby written into
     * a buffer for encrypted data, or the SSL engine generates data and writes it into a buffer for encrytped data
     * to advance an SSL handshake (including renegotiation).
     * The method call may produce encrypted data, but there is no guarantee that it will, even if LINSTOR messages
     * are pending, because the SSL engine might require data to be sent to the network first in order to empty the
     * target buffer of the operation, or might need to resize the buffers involved in the operation.
     * If encrypted data is produced by the method call, an attempt is made to write (send) the data to the network.
     *
     * Calling the method when the network is not ready to send more data may pass additional data through the SSL
     * engine if there is enough free space in the buffers involved, but may not send any data to the network.
     * Although doing that does not cause any malfunctions, it should be considered an implementation error, because
     * it is an inefficient and unnecessary operation.
     *
     * The method call may change the set of I/O operations that the peer object is waiting for, or may produce one
     * or multiple asynchronously running SSL engine tasks in separate threads and suspend all I/O operations. Upon
     * completion of those SSL engine tasks, the peer object will be placed into a connector service queue, and the
     * connector service will be notified to continue SSL processing for this peer object.
     *
     * The method call may dequeue one or multiple outbound LINSTOR messages. The absence of any pending or queued
     * outbound LINSTOR message does not mean that the message has been sent, because encrypted data generated from
     * the message's unencrypted data may still be buffered for writing (sending).
     *
     * @param outChannel The network channel associated with this peer object
     * @return WriteState object describing the result of the write operation; not used in the current implementation;
     *         always returns <code>WriteState.UNFINISHED</code>
     * @throws IllegalMessageStateException If the LINSTOR message is in illegal state for reading its data;
     *         not supposed to happen, indicates an implementation error
     * @throws IOException If the network write operation fails, or if SSL processing fails
     */
    @Override
    public WriteState write(@Nonnull final SocketChannel outChannel)
        throws IllegalMessageStateException, IOException
    {
        WriteState state = WriteState.UNFINISHED;

        encryptedWriteBuffer.compact();

        SSLEngineResult.Status trafficStatus = null;
        SSLEngineResult.HandshakeStatus hsStatus =  SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
        ioRequest = false;
        while (!ioRequest)
        {
            hsStatus = sslEngine.getHandshakeStatus();
            if (hsStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
            { // SSL encrypt application data
                sslReady = true;
                trafficStatus = sslOutbound(false);
            }
            else
            {
                trafficStatus = sslHandshake(hsStatus, true);
            }

            if (trafficStatus == SSLEngineResult.Status.CLOSED)
            {
                // Typically from an unwrap/inbound operation; SSL shutdown not implemented as of Nov 21, 2023,
                // LINSTOR just closes the network connection.
                // Partial implementation for possible future use.
                sslEngine.closeInbound();
                closeConnection(true);
            }
        }

        // Send encrypted data
        encryptedWriteBuffer.flip();
        final int writeSize = outChannel.write(encryptedWriteBuffer);
        if (encryptedWriteBuffer.hasRemaining())
        {
            // Case 1: Not all data could be sent, make sure OP_WRITE is enabled, so the rest of the data can
            //         be sent later
            enableInterestOps(SelectionKey.OP_WRITE);
        }
        else
        if (hsStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP)
        {
            // Case 2: The SSL handshake has produced data that was sent, but needs to be invoked again, because
            //         it might need to send data again
            enableInterestOps(SelectionKey.OP_WRITE);
        }
        else
        if (hsStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING &&
            hsStatus != SSLEngineResult.HandshakeStatus.FINISHED)
        { // SSL handshake in progress
            disableInterestOps(SelectionKey.OP_WRITE);
        }
        else
        if (msgOut != null)
        { // SSL handshake not in progress and more application messages pending
            enableInterestOps(SelectionKey.OP_WRITE);
        }
        else
        {
            disableInterestOps(SelectionKey.OP_WRITE);
        }

        return state;
    }

    /**
     * Passes unencrypted data through the SSL engine or advances the SSL handshake.
     * Outbound LINSTOR message data is passed through the SSL engine, potentially writing encrypted data into
     * a buffer for writing (sending) to the network. If an SSL handshake is in progess, this method must be
     * called with the <code>forceWrap</code> argument set to <code>true</code>. This ensures that the
     * SSL engine's <code>wrap</code> method is always called, and is required for advancing the SSL handshake
     * if no outbound LINSTOR messages are pending.
     * If no SSL handshake is in progess and no outbound LINSTOR messages are pending, then this method does
     * not produce any encrypted data for sending.
     *
     * @param forceWrap performs an SSL engine <code>wrap</code> method call independent of the availability of
     *        outbound LINSTOR message data
     * @return result of an SSLEngine wrap operation, or null if no wrap operations were performed
     * @throws SSLException if the JCL SSL engine generates the exception
     * @throws IllegalMessageStateException If the LINSTOR message is in an illegal state for reading its data;
     *         not supposed to happen, indicates an implementation error
     */
    private SSLEngineResult.Status sslOutbound(final boolean forceWrap)
        throws SSLException, IllegalMessageStateException
    {
        SSLEngineResult sslStatus = null;
        if (msgOut != null)
        {
            switch (currentWritePhase)
            {
                case HEADER:
                    final ByteBuffer headerBuffer = msgOut.getHeaderBuffer();
                    sslStatus = sslEngine.wrap(headerBuffer, encryptedWriteBuffer);
                    if (!headerBuffer.hasRemaining())
                    {
                        currentWritePhase = currentWritePhase.getNextPhase();
                    }
                    break;
                case DATA:
                    final ByteBuffer dataBuffer = msgOut.getDataBuffer();
                    sslStatus = sslEngine.wrap(dataBuffer, encryptedWriteBuffer);
                    if (!dataBuffer.hasRemaining())
                    {
                        currentWritePhase = Phase.HEADER;
                        // If there are no more outbound messages pending, this will disable OP_WRITE.
                        nextOutMessage();
                        // WriteState.FINISHED would be returned here to indicate that an entire message
                        // has been sent, if it were to be implemented in the netcom connector
                    }
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
        }
        else
        {
            if (forceWrap)
            { // The SSL handshake requires an SSLEngine wrap operation, but there was no application data to wrap
                sslStatus = sslEngine.wrap(dummyBuffer, encryptedWriteBuffer);
            }
            ioRequest = true;
        }

        if (sslStatus != null)
        {
            SSLEngineResult.Status trafficStatus = sslStatus.getStatus();
            if (trafficStatus == SSLEngineResult.Status.BUFFER_OVERFLOW)
            {
                if (encryptedWriteBuffer.position() == 0)
                { // Buffer is too small
                    adjustEncryptedWriteBuffer();
                }
                else
                { // Too much data in the buffer
                    enableInterestOps(SelectionKey.OP_WRITE);
                    ioRequest = true;
                }
            }
        }
        return sslStatus == null ? null : sslStatus.getStatus();
    }

    /**
     * Called by the connector service upon completion of asynchronously running SSL engine tasks.
     * This callback method continues the SSL handshake after the peer's I/O operations had been suspended
     * because the peer object had to wait for the completion of asynchronously running SSL engine tasks.
     *
     * @throws SSLException if the JCL SSL engine generates the exception
     * @throws IllegalMessageStateException If the LINSTOR message is in an illegal state for the data transfer
     *         operation performed by SSL processing methods; not supposed to happen, indicates an implementation error
     * @throws IOException If network I/O operations or SSL processing operations fail
     */
    protected void sslTasksCompleted()
        throws SSLException, IllegalMessageStateException, IOException
    {
        SSLEngineResult.HandshakeStatus hsStatus = sslEngine.getHandshakeStatus();
        SSLEngineResult.Status trafficStatus = sslHandshake(hsStatus, false);
        if (trafficStatus == SSLEngineResult.Status.CLOSED)
        {
            sslEngine.closeInbound();
            closeConnection(true);
        }
    }

    /**
     * Performs a step of an SSL handshake
     *
     * @param hsStatus Current SSLEngine handshake status
     * @param writeReady true if sslHandshake is called in preparation of a write operation (aka sending data)
     * @return result of SSLEngine wrap/unwrap operations, if any were performed, otherwise null
     * @throws SSLException if the JCL SSL implementation generates the exception
     * @throws IllegalMessageStateException If the LINSTOR message is in an illegal state for the data transfer
     *         operation performed by SSL processing methods; not supposed to happen, indicates an implementation error
     * @throws IOException If network I/O operations or SSL processing operations fail
     */
    private SSLEngineResult.Status sslHandshake(
        @Nonnull SSLEngineResult.HandshakeStatus hsStatus,
        final boolean writeReady
    )
        throws SSLException, IllegalMessageStateException, IOException
    {
        SSLEngineResult.Status trafficStatus = null;
        if (hsStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP)
        { // SSL handshake needs to produce and send outbound data
            if (writeReady)
            {
                trafficStatus = sslOutbound(true);
            }
            else
            {
                enableInterestOps(SelectionKey.OP_WRITE);
                ioRequest = true;
            }
        }
        else
        if (hsStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP ||
            hsStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN)
        { // SSL handshake needs to receive data
            trafficStatus = sslInbound();
        }
        else
        if (hsStatus == SSLEngineResult.HandshakeStatus.NEED_TASK)
        { // SSL handshake requires the result of a task that can run asynchronously
            executeSslTasks();
            // Does not actually need I/O, but the SSL handshake needs to wait for the
            // the asynchronously executing SSL tasks to complete before the SSL handshake loop
            // can continue, and the I/O code outside of the SSL handshake code handles the
            // wait/wakeup cycle for asynchronously executing SSL tasks. Therefore, drop
            // out of the SSL handshake loop and return to the I/O handling code.
            ioRequest = true;
        }
        else
        if (hsStatus == SSLEngineResult.HandshakeStatus.FINISHED ||
            hsStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
        { // SSL handshake successful
            sslReady = true;
            // Update the timestamp as if a ping/pong cycle had been completed, so that the peer is not
            // kicked out by the TaskScheduleService/PingTask if the connection/handshake process took long
            pongReceived();
        }
        else
        { // Error
            throw new ImplementationError(
                "Unknown SSL handshake status, SSLEngineResult.HandshakeStatus ordinal = " + hsStatus.name()
            );
        }
        return trafficStatus;
    }

    /**
     * Transfers data from buffered unencrypted inbound data into new inbound LINSTOR messages.
     * If enough data is available, this method will create and queue one or multiple inbound LINSTOR messages.
     *
     * @throws IllegalMessageStateException If the LINSTOR message is in illegal state for writing data into it;
     *         not supposed to happen, indicates an implementation error
     */
    private void extractMessages()
        throws IllegalMessageStateException
    {
        plainReadBuffer.flip();
        while (plainReadBuffer.hasRemaining())
        {
            switch (currentReadPhase)
            {
                case HEADER:
                    final ByteBuffer headerBuffer = msgIn.getHeaderBuffer();
                    transferBufferData(plainReadBuffer, headerBuffer);
                    if (!headerBuffer.hasRemaining())
                    {
                        currentReadPhase = currentReadPhase.getNextPhase();
                        initDataByteBuffer(headerBuffer);
                    }
                    break;
                case DATA:
                    final ByteBuffer dataBuffer = msgIn.getDataBuffer();
                    transferBufferData(plainReadBuffer, dataBuffer);
                    if (!dataBuffer.hasRemaining())
                    {
                        currentReadPhase = currentReadPhase.getNextPhase();
                        addToQueue(msgIn);
                        rdState = ReadState.FINISHED;
                        msgIn = createMessage(false);
                    }
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
        }
    }

    /**
     * Transfers data from one buffer into another buffer.
     * Copies as much data as is available in the source buffer, or as much data as the destination buffer
     * is able to receive, whichever is the smaller value, as determined by the current position and the
     * current limit of both buffers, from the source buffer into the destination buffer, and updates the
     * position of both buffers accordingly.
     *
     * @param srcBuffer Source buffer, the buffer to copy data from
     * @param dstBuffer Destination buffer, the buffer to copy data into
     */
    private void transferBufferData(@Nonnull final ByteBuffer srcBuffer, @Nonnull final ByteBuffer dstBuffer)
    {
        final int srcOffset = srcBuffer.position();
        final int srcRemain = srcBuffer.remaining();

        final int dstRemain = dstBuffer.remaining();

        if (srcRemain > dstRemain)
        {
            final int savedLimit = srcBuffer.limit();
            srcBuffer.limit(srcOffset + dstRemain);
            dstBuffer.put(srcBuffer);
            srcBuffer.limit(savedLimit);
        }
        else
        {
            dstBuffer.put(srcBuffer);
        }
    }

    /**
     * Reallocates encryptedWriteBuffer according to current SSL packet buffer size, if necessary.
     * The buffer must be empty when this method is called.<br/>
     * <br/>
     * If a reallocation is required, the reallocated buffer replaces the current buffer. The reallocated
     * buffer's position is set to zero and its limit is set to its capacity.
     */
    private void adjustEncryptedWriteBuffer()
    {
        // Buffer must be empty when this method is called
        final SSLSession session = sslEngine.getSession();
        final int reqSize = session.getPacketBufferSize();
        if (reqSize > encryptedWriteBuffer.capacity())
        {
            encryptedWriteBuffer = ByteBuffer.allocate(reqSize);
        }
    }

    /**
     * Reallocates encryptedReadBuffer according to the current SSL packet buffer size, if necessary.
     * The buffer's position must be zero, and the buffer's limit must be set to the length of the data contained
     * in the buffer when this method is called.<br/>
     * <br/>
     * If a reallocation is required:<br/>
     * The data in the current buffer is copied to the reallocated buffer. The reallocated buffer replaces the
     * current buffer, its position is set to zero, and its limit is set to the length of the data contained
     * in it (the same state that the original buffer was in when the method was called).
     */
    private void adjustEncryptedReadBuffer()
    {
        // Buffer is full when this method is called
        final SSLSession session = sslEngine.getSession();
        final int reqSize = session.getPacketBufferSize();
        if (reqSize > encryptedReadBuffer.capacity())
        {
            final ByteBuffer resizedBuffer = ByteBuffer.allocate(reqSize);
            resizedBuffer.put(encryptedReadBuffer);
            encryptedReadBuffer = resizedBuffer;
            encryptedReadBuffer.flip();
        }
    }

    /**
     * Reallocates plainReadBuffer according to the current SSL application buffer size, if necessary.
     * The buffer must be empty when this method is called.<br/>
     * <br/>
     * If a reallocation is required, the reallocated buffer replaces the current buffer. The reallocated
     * buffer's position is set to zero and its limit is set to its capacity.
     */
    private void adjustPlainReadBuffer()
    {
        // Buffer must be empty when this method is called
        final SSLSession session = sslEngine.getSession();
        final int reqSize = session.getApplicationBufferSize();
        if (reqSize > plainReadBuffer.capacity())
        {
            plainReadBuffer = ByteBuffer.allocate(reqSize);
        }
    }

    /**
     * Schedules all of the SSL engine's pending tasks on concurrently running threads that are spawned by this
     * method for that purpose.
     *
     * @throws IOException If delegation of a task fails
     */
    private void executeSslTasks()
        throws IOException
    {
        Runnable sslTask = sslEngine.getDelegatedTask();
        sslTaskLock.lock();
        try
        {
            while (sslTask != null)
            {
                ++activeTaskCount;
                try
                {
                    final Runnable curSslTask = sslTask;
                    final Thread sslTaskThread = new Thread(
                        () ->
                        {
                            try
                            {
                                curSslTask.run();
                            }
                            catch (Exception ignored)
                            {
                            }
                            finally
                            {
                                boolean tasksComplete = false;
                                boolean tasksCanceled = false;
                                sslTaskLock.lock();
                                --activeTaskCount;
                                tasksComplete = activeTaskCount <= 0;
                                tasksCanceled = sslTasksCanceled;
                                sslTaskLock.unlock();

                                if (tasksComplete)
                                {
                                    if (!tasksCanceled)
                                    {
                                        final SslTcpConnectorService peerConnector =
                                            (SslTcpConnectorService) getConnector();
                                        peerConnector.taskCompleted(this);
                                    }
                                }
                            }
                        }
                    );
                    sslTaskThread.start();
                }
                catch (Exception exc)
                {
                    --activeTaskCount;
                    throw new IOException("SSL protocol error: Unable to execute delegated task", exc);
                }
                sslTask = sslEngine.getDelegatedTask();
            }
        }
        finally
        {
            sslTaskLock.unlock();
        }
    }

    /**
     * Cancels the concurrently running SSL engine tasks.
     * This prevents the peer from continuing SSL processing when the delegated tasks finish. This method
     * should be called when the peer's connection is closed.
     */
    protected void cancelSslTasks()
    {
        sslTaskLock.lock();
        try
        {
            sslTasksCanceled = true;
        }
        finally
        {
            sslTaskLock.unlock();
        }
    }

    /**
     * Returns the SelectionKey object associated with this peer.
     *
     * @return SelectionKey object
     */
    @Override
    protected SelectionKey getSelectionKey()
    {
        return super.getSelectionKey();
    }
}
