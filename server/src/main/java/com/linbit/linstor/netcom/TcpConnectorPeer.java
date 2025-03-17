package com.linbit.linstor.netcom;

import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.prometheus.LinstorServerMetrics;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.utils.OrderingFlux;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import io.prometheus.client.Histogram;
import org.reactivestreams.Publisher;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * Tracks the status of the communication with a peer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpConnectorPeer implements Peer
{
    public enum Phase
    {
        HEADER,
        DATA;

        public Phase getNextPhase()
        {
            return DATA;
        }
    }

    public enum ReadState
    {
        UNFINISHED,
        FINISHED,
        END_OF_STREAM
    }

    public enum WriteState
    {
        UNFINISHED,
        FINISHED
    }

    private final Node node;

    private final ErrorReporter errorReporter;

    private final CommonSerializer commonSerializer;

    private final String peerId;

    private final TcpConnector connector;

    // Current inbound message
    protected Message msgIn;

    // Current outbound message; cached for quicker access
    protected Message msgOut;

    // Queue of pending outbound messages
    // TODO: Put a capacity limit on the maximum number of queued outbound messages
    protected final Deque<Message> msgOutQueue;

    protected SelectionKey selKey;

    private AccessContext peerAccCtx;

    private Object attachment;

    protected volatile boolean connected = false;
    protected ApiConsts.ConnectionStatus connectionStatus = ApiConsts.ConnectionStatus.OFFLINE;
    protected boolean authenticated = false;
    protected boolean fullSyncApplied = false;
    protected boolean fullSyncFailed = false;

    // Volatile guarantees atomic read and write
    //
    // The counters are only incremented by only one thread
    // at a time, but may be concurrently read by multiple threads,
    // therefore requiring atomic read and write
    private volatile long msgSentCtr = 0;
    private volatile long msgRecvCtr = 0;
    private volatile long msgSentSizePeak = 0;
    private volatile long msgRecvSizePeak = 0;

    protected long lastPingSent = -1;
    private long lastPongReceived = -1;

    protected final Message internalPingMsg;
    protected final Message internalPongMsg;

    private final ReadWriteLock satelliteStateLock;
    private SatelliteState satelliteState;

    private long fullSyncId;
    private final AtomicLong serializerId;
    private final ReadWriteLock serializerLock;

    protected Phase currentReadPhase = Phase.HEADER;
    protected Phase currentWritePhase = Phase.HEADER;

    private final Queue<Message> finishedMsgInQueue;
    private int opInterest = OP_READ;

    private final AtomicLong nextIncomingMessageSeq = new AtomicLong();
    private final Sinks.Many<Tuple2<Long, Publisher<?>>> incomingMessageSink;

    private final AtomicLong nextApiCallId = new AtomicLong(1);
    private final Map<Long, FluxSink<ByteArrayInputStream>> openRpcs = Collections.synchronizedMap(new TreeMap<>());

    private final ExtToolsManager externalToolsManager = new ExtToolsManager();
    private StltConfig stltConfig = new StltConfig();

    private final Map<String, Property> dynamicProperties = new HashMap<>();

    private final static long CLOSE_FLUSH_TIMEOUT_MS = 500;

    protected TcpConnectorPeer(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        String peerIdRef,
        TcpConnector connectorRef,
        SelectionKey key,
        AccessContext accCtx,
        Node nodeRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        peerId = peerIdRef;
        connector = connectorRef;
        node = nodeRef;
        msgOutQueue = new LinkedList<>();

        // Do not use createMessage() here!
        // The SslTcpConnectorPeer has not initialized SSLEngine instance yet,
        // so a NullPointerException would be thrown in createMessage().
        // After initialization of the sslEngine, msgIn will be overwritten with
        // a reference to a valid instance.
        msgIn = new MessageData(false);

        selKey = key;
        peerAccCtx = accCtx;
        attachment = null;

        internalPingMsg = new TcpHeaderOnlyMessage(MessageTypes.PING);
        internalPongMsg = new TcpHeaderOnlyMessage(MessageTypes.PONG);

        serializerId = new AtomicLong(0);
        serializerLock = new ReentrantReadWriteLock(true);

        satelliteStateLock = new ReentrantReadWriteLock(true);
        if (node != null)
        {
            satelliteState = new SatelliteState();
        }

        finishedMsgInQueue = new LinkedList<>();

        incomingMessageSink = Sinks.many().unicast().onBackpressureBuffer();
        Flux<Tuple2<Long, Publisher<?>>> flux = incomingMessageSink.asFlux();
        flux
            .transform(OrderingFlux::order)
            .flatMap(Function.identity(), Integer.MAX_VALUE)
            .subscribe(
                ignored ->
                {
                    // do nothing
                },
                exc -> errorReporterRef.reportError(
                    exc, null, null, "Uncaught exception in processor for peer '" + this + "'")
            );
    }

    protected ErrorReporter getErrorReporter()
    {
        return errorReporter;
    }

    @Override
    public String getId()
    {
        return peerId;
    }

    @Override
    public ServiceName getConnectorInstanceName()
    {
        ServiceName connInstName = null;
        if (connector != null)
        {
            connInstName = connector.getInstanceName();
        }
        return connInstName;
    }

    @Override
    public Node getNode()
    {
        return node;
    }

    @Override
    public void waitUntilConnectionEstablished() throws InterruptedException
    {
        while (!connected)
        {
            synchronized (this)
            {
                if (!connected)
                {
                    this.wait();
                }
            }
        }
    }

    @Override
    public void connectionEstablished() throws SSLException
    {
        connected = true;
        pongReceived(); // in order to calculate the first "real" pong correctly.

        if (node != null)
        {
            node.connectionEstablished();
        }
        synchronized (this)
        {
            this.notifyAll();
        }
    }

    @Override
    public Message createMessage()
    {
        return createMessage(true);
    }

    protected Message createMessage(boolean forSend)
    {
        if (forSend)
        {
            currentWritePhase = Phase.HEADER;
        }
        else
        {
            currentReadPhase = Phase.HEADER;
        }
        return new MessageData(forSend);
    }

    @Override
    public boolean sendMessage(Message msg)
        throws IllegalMessageStateException
    {
        boolean connFlag = connected;
        if (connFlag)
        {
            synchronized (this)
            {
                long msgSize = msg.getData().length;
                if (msgSize > msgSentSizePeak)
                {
                    msgSentSizePeak = msgSize;
                }

                // Queue the message for sending
                if (msgOut == null)
                {
                    msgOut = msg;
                }
                else
                {
                    msgOutQueue.add(msg);
                }

                try
                {
                    enableInterestOps(OP_WRITE);
                    connector.wakeup();
                }
                catch (IllegalStateException illState)
                {
                    // No-op; Subclasses of illState can be thrown
                    // when the connection has been closed
                }
            }
        }
        return connFlag;
    }

    /**
     * Adds the specified I/O operations to this peer's I/O operations interest set.
     * @param ops <code>SelectionKey</code> I/O operations - ACCEPT, CONNECT, READ, WRITE
     */
    protected void enableInterestOps(final int ops)
    {
        opInterest |= ops;
        selKey.interestOps(opInterest);
    }

    /**
     * Indicates whether this peer's interest set contains all of the specified I/O operations.
     * @param ops <code>SelectionKey</code> I/O operations - ACCEPT, CONNECT, READ, WRITE
     * @return true if the interest set contains all of the specified operations
     */
    protected boolean hasInterestOps(final int ops)
    {
        return (opInterest & ops) == ops;
    }

    /**
     * Removes the specified I/O operations from this peer's I/O operations interest set.
     * @param ops <code>SelectionKey</code> I/O operations - ACCEPT, CONNECT, READ, WRITE
     */
    protected void disableInterestOps(final int ops)
    {
        opInterest &= ~ops;
        selKey.interestOps(opInterest);
    }

    /**
     * Sets this peer's I/O operations interest set to match the specified I/O operations.
     * @param ops <code>SelectionKey</code> I/O operations - ACCEPT, CONNECT, READ, WRITE
     */
    protected void setInterestOps(final int ops)
    {
        opInterest = ops;
        selKey.interestOps(ops);
    }

    @Override
    public boolean sendMessage(byte[] data)
    {
        boolean isConnected = false;
        try
        {
            Message msg = createMessage();
            msg.setData(data);
            isConnected = sendMessage(msg);
        }
        catch (IllegalMessageStateException exc)
        {
            throw new ImplementationError(
                "Creating an outgoing message caused an IllegalMessageStateException",
                exc
            );
        }
        return isConnected;
    }

    @Override
    public boolean sendMessage(byte[] data, String apiCall)
    {
        if (apiCall != null)
        {
            errorReporter.logTrace("Sending message to Peer '%s': %s", toString(), apiCall);
        }
        return sendMessage(data);
    }

    @Override
    public long getNextIncomingMessageSeq()
    {
        return nextIncomingMessageSeq.getAndIncrement();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void processInOrder(long peerSeq, Publisher<?> publisher)
    {
        // Since reactor 3.4.x, it doesn't busy loop anymore itself, but rather let that be done by
        // the library user see: https://github.com/reactor/reactor-core/issues/2049
        Sinks.EmitResult emitRes = incomingMessageSink.tryEmitNext(Tuples.of(peerSeq, publisher));
        while (emitRes == Sinks.EmitResult.FAIL_NON_SERIALIZED)
        {
            LockSupport.parkNanos(10);
            emitRes = incomingMessageSink.tryEmitNext(Tuples.of(peerSeq, publisher));
        }
        if (emitRes.isFailure())
        {
            errorReporter.logError("Unable to emit process");
        }
    }

    @Override
    public Flux<ByteArrayInputStream> apiCall(String apiCallName, byte[] data)
    {
        return apiCall(apiCallName, data, true, true);
    }

    public Flux<ByteArrayInputStream> apiCall(
        String apiCallName,
        byte[] data,
        boolean authenticationRequired,
        boolean fullSyncAppliedRequired
    )
    {
        Flux<ByteArrayInputStream> call = Flux
            .<ByteArrayInputStream>create(fluxSink ->
                {
                    MDC.setContextMap(MDC.getCopyOfContextMap());
                    long apiCallId = nextApiCallId.getAndIncrement();
                    byte[] messageBytes = commonSerializer.apiCallBuilder(apiCallName, apiCallId).bytes(data).build();

                    fluxSink.onDispose(() -> openRpcs.remove(apiCallId));

                    openRpcs.put(apiCallId, fluxSink);

                    if (authenticationRequired && !authenticated || fullSyncAppliedRequired && !fullSyncApplied)
                    {
                        fluxSink.error(new PeerNotConnectedException());
                    }
                    else
                    {
                        errorReporter.logTrace("Peer %s, API call %d '%s' send", this, apiCallId, apiCallName);
                        boolean isConnected = sendMessage(messageBytes);
                        if (!isConnected)
                        {
                            fluxSink.error(new PeerNotConnectedException());
                        }
                    }
                }
            )
            .switchIfEmpty(Flux.error(new ApiCallNoResponseException()));

        return Flux.using(
            () -> LinstorServerMetrics.apiCallHistogram.labels(apiCallName, peerId).startTimer(),
            (ignored) -> call,
            Histogram.Timer::close
        );
    }

    @Override
    public void apiCallAnswer(long apiCallId, ByteArrayInputStream data)
    {
        FluxSink<ByteArrayInputStream> rpcSink = openRpcs.get(apiCallId);
        if (rpcSink == null)
        {
            errorReporter.logDebug("Unexpected API call answer received");
        }
        else
        {
            rpcSink.next(data);
        }
    }

    @Override
    public void apiCallError(long apiCallId, Throwable exc)
    {
        FluxSink<ByteArrayInputStream> rpcSink = openRpcs.get(apiCallId);
        if (rpcSink == null)
        {
            errorReporter.logDebug("Unexpected API call error received");
        }
        else
        {
            rpcSink.error(exc);
        }
    }

    @Override
    public void apiCallComplete(long apiCallId)
    {
        FluxSink<ByteArrayInputStream> rpcSink = openRpcs.get(apiCallId);
        if (rpcSink == null)
        {
            errorReporter.logDebug("Unexpected API call %d completion received", apiCallId);
        }
        else
        {
            rpcSink.complete();
        }
    }

    @Override
    public TcpConnector getConnector()
    {
        return connector;
    }

    /**
     * Closes the connection to the peer
     *
     * FIXME: This said "throws SSLException", but JavaDoc complains. Check whether it actually throw SSLException
     *        and whether it is even a good idea to do that.
     */
    @Override
    public void closeConnection()
    {
        connector.closeConnection(this, false);
    }

    @Override
    public void closeConnection(boolean allowReconnect)
    {
        connector.closeConnection(this, allowReconnect);
    }

    private void waitOutMessagesFlushed()
    {
        // wait up to CLOSE_FLUSH_TIMEOUT_MS for out messages to be flushed
        long start = System.currentTimeMillis();
        synchronized (this)
        {
            while (!msgOutQueue.isEmpty() || msgOut != null)
            {
                // wait till
                try
                {
                    this.wait(10);
                }
                catch (InterruptedException exc)
                {
                    errorReporter.reportError(exc);
                    break;
                }

                if (System.currentTimeMillis() >= start + CLOSE_FLUSH_TIMEOUT_MS)
                {
                    break;
                }
            }
        }
    }

    @Override
    public void connectionClosing()
    {
        connected = false;
        waitOutMessagesFlushed();

        authenticated = false;

        // deactivate all interest in READ or WRITE operations
        setInterestOps(0);

        synchronized (openRpcs)
        {
            // preventing ConcurrentModificationException with "#apiCall's fluxSink.onDispose(...openRpcs.remove(...))
            Set<FluxSink<ByteArrayInputStream>> copyOpenRpcsSet = new HashSet<>(openRpcs.values());
            for (FluxSink<ByteArrayInputStream> rpcSink : copyOpenRpcsSet)
            {
                rpcSink.error(new PeerClosingConnectionException());
            }
            openRpcs.clear(); // basically no-op, more for documentation purpose
        }
    }

    @Override
    public boolean isOnline()
    {
        return isConnected(true);
    }

    @Override
    public ApiConsts.ConnectionStatus getConnectionStatus()
    {
        return connectionStatus;
    }

    @Override
    public void setConnectionStatus(ApiConsts.ConnectionStatus status)
    {
        if (node == null)
        {
            errorReporter.logInfo(
                "Changing connection state of peer '%s' from %s -> %s",
                getId(),
                connectionStatus,
                status
            );
        }
        else
        {
            errorReporter.logInfo(
                "Changing connection state of node '%s' from %s -> %s",
                node.getName(),
                connectionStatus,
                status
            );
        }
        connectionStatus = status;
    }

    @Override
    public boolean isConnected(boolean ensureAuthenticated)
    {
        boolean ret = false;
        if (connected)
        {
            if (ensureAuthenticated)
            {
                if (authenticated && !hasFullSyncFailed())
                {
                    ret = true;
                }
            }
            else
            {
                ret = true;
            }
        }
        return ret;
    }

    @Override
    public boolean isAuthenticated()
    {
        return authenticated;
    }

    @Override
    public void setAuthenticated(boolean authenticatedFlag)
    {
        authenticated = authenticatedFlag;
    }

    protected void nextInMessage()
    {
        msgIn = createMessage(false);
        ++msgRecvCtr;
    }

    protected SelectionKey getSelectionKey()
    {
        return selKey;
    }

    void setSelectionKey(SelectionKey selKeyRef)
    {
        selKey = selKeyRef;
    }

    protected void nextOutMessage()
    {
        synchronized (this)
        {
            msgOut = msgOutQueue.pollFirst();
            if (msgOut == null)
            {
                try
                {
                    // No more outbound messages present, disable OP_WRITE
                    disableInterestOps(OP_WRITE);
                }
                catch (IllegalStateException illState)
                {
                    // No-op; Subclasses of illState can be thrown
                    // when the connection has been closed
                }
            }
            ++msgSentCtr;
        }
    }

    @Override
    public AccessContext getAccessContext()
    {
        return peerAccCtx;
    }

    @Override
    public void setAccessContext(AccessContext privilegedCtx, AccessContext newAccCtx)
        throws AccessDeniedException
    {
        privilegedCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        peerAccCtx = newAccCtx;
    }

    @Override
    public void attach(Object attachmentRef)
    {
        attachment = attachmentRef;
    }

    @Override
    public Object getAttachment()
    {
        return attachment;
    }

    @Override
    public int outQueueCapacity()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public int outQueueCount()
    {
        return msgOutQueue.size();
    }

    @Override
    public long msgSentCount()
    {
        return msgSentCtr;
    }

    @Override
    public long msgRecvCount()
    {
        return msgRecvCtr;
    }

    @Override
    public long msgSentMaxSize()
    {
        return msgSentSizePeak;
    }

    @Override
    public long msgRecvMaxSize()
    {
        return msgRecvSizePeak;
    }

    @Override
    public InetSocketAddress peerAddress()
    {
        InetSocketAddress peerAddr = null;
        try
        {
            @SuppressWarnings("resource")
            SelectableChannel channel = selKey.channel();
            if (channel != null && channel instanceof SocketChannel)
            {
                SocketChannel sockChannel = (SocketChannel) channel;
                SocketAddress sockAddr = sockChannel.getRemoteAddress();
                if (sockAddr != null && sockAddr instanceof InetSocketAddress)
                {
                    peerAddr = (InetSocketAddress) sockAddr;
                }
            }
        }
        catch (IOException ignored)
        {
            // Undeterminable address (possibly closed or otherwise invalid)
            // No-op; method returns null
        }
        return peerAddr;
    }

    @Override
    public InetSocketAddress localAddress()
    {
        InetSocketAddress localAddr = null;
        try
        {
            @SuppressWarnings("resource")
            SelectableChannel channel = selKey.channel();
            if (channel != null && channel instanceof SocketChannel)
            {
                SocketChannel sockChannel = (SocketChannel) channel;
                SocketAddress sockAddr = sockChannel.getLocalAddress();
                if (sockAddr != null && sockAddr instanceof InetSocketAddress)
                {
                    localAddr = (InetSocketAddress) sockAddr;
                }
            }
        }
        catch (IOException ignored)
        {
            // Undeterminable address (possibly closed or otherwise invalid)
            // No-op; method returns null
        }
        return localAddr;
    }

    @Override
    public void sendPing()
    {
        try
        {
            sendMessage(getInternalPingMessage());
        }
        catch (IllegalMessageStateException illegalMsgStateExc)
        {
            throw new ImplementationError(illegalMsgStateExc);
        }
        lastPingSent = System.currentTimeMillis();
    }

    protected Message getInternalPingMessage()
    {
        return internalPingMsg;
    }

    @Override
    public void sendPong()
    {
        try
        {
            sendMessage(getInternalPongMessage());
        }
        catch (IllegalMessageStateException illegalMsgStateExc)
        {
            throw new ImplementationError(illegalMsgStateExc);
        }
    }

    protected Message getInternalPongMessage()
    {
        return internalPongMsg;
    }

    @Override
    public void pongReceived()
    {
        lastPongReceived = System.currentTimeMillis();
    }

    @Override
    public long getLastPingSent()
    {
        return lastPingSent;
    }

    @Override
    public long getLastPongReceived()
    {
        return lastPongReceived;
    }

    @Override
    public ReadWriteLock getSatelliteStateLock()
    {
        return satelliteStateLock;
    }

    @Override
    public SatelliteState getSatelliteState()
    {
        return satelliteState;
    }

    @Override
    public void setFullSyncId(long id)
    {
        fullSyncId = id;
    }
    @Override
    public long getFullSyncId()
    {
        return fullSyncId;
    }
    @Override
    public long getNextSerializerId()
    {
        return serializerId.getAndIncrement();
    }
    @Override
    public ReadWriteLock getSerializerLock()
    {
        return serializerLock;
    }

    @Override
    public void fullSyncFailed(@Nullable ApiConsts.ConnectionStatus suggestedConnStatusRef)
    {
        fullSyncFailed = true;
        // just to be sure, that even if some component still sends an update, it should be
        // an invalid one. -1 will make it look like an out-dated update for the satellite.
        fullSyncId = -1;
        if (suggestedConnStatusRef == null)
        {
            setConnectionStatus(ApiConsts.ConnectionStatus.FULL_SYNC_FAILED);
        }
        else
        {
            setConnectionStatus(suggestedConnStatusRef);
        }
    }

    @Override
    public boolean hasFullSyncFailed()
    {
        return fullSyncFailed;
    }

    @Override
    public void fullSyncApplied()
    {
        fullSyncApplied = true;
    }

    @Override
    public boolean isFullSyncApplied()
    {
        return fullSyncApplied;
    }

    @Override
    public boolean hasNextMsgIn()
    {
        return !finishedMsgInQueue.isEmpty();
    }

    @Override
    public Message nextCurrentMsgIn()
    {
        Message message = finishedMsgInQueue.poll();
        if (finishedMsgInQueue.size() < MAX_INCOMING_QUEUE_SIZE && !hasInterestOps(OP_READ))
        {
            enableInterestOps(OP_READ);
        }
        return message;
    }

    @Override
    public String toString()
    {
        String ret;
        if (node == null || node.isDeleted())
        {
            ret = getId();
        }
        else
        {
            ret = "Node: '" + node.getName().displayValue + "'";
        }
        return ret;
    }

    public ReadState read(SocketChannel inChannel)
        throws IllegalMessageStateException, IOException
    {
        ReadState state = ReadState.UNFINISHED;
        switch (currentReadPhase)
        {
            case HEADER:
                {
                    ByteBuffer headerBuffer = msgIn.getHeaderBuffer();
                    int readCount = inChannel.read(headerBuffer);
                    if (readCount > -1)
                    {
                        if (!headerBuffer.hasRemaining())
                        {
                            // All header data has been received
                            // Prepare reading the message
                            initDataByteBuffer(headerBuffer);
                            state = readData(inChannel, state);
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
                state = readData(inChannel, state);
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
        if (state == ReadState.FINISHED)
        {
            addToQueue(msgIn);
            nextInMessage();
        }
        return state;
    }

    protected void initDataByteBuffer(ByteBuffer headerBuffer) throws IllegalMessageStateException
    {
        int dataSize = headerBuffer.getInt(Message.LENGTH_FIELD_OFFSET);
        if (dataSize < 0)
        {
            dataSize = 0;
        }
        if (dataSize > Message.DEFAULT_MAX_DATA_SIZE)
        {
            dataSize = Message.DEFAULT_MAX_DATA_SIZE;
        }
        msgIn.setData(new byte[dataSize]);
        currentReadPhase = currentReadPhase.getNextPhase();
    }

    private ReadState readData(SocketChannel inChannel, ReadState stateRef)
        throws IOException, IllegalMessageStateException
    {
        ReadState state = stateRef;
        ByteBuffer dataBuffer = msgIn.getDataBuffer();
        int readCount = inChannel.read(dataBuffer);
        if (readCount <= -1)
        {
            // Peer has closed the stream
            state = ReadState.END_OF_STREAM;
        }
        if (!dataBuffer.hasRemaining())
        {
            // All message data has been received
            currentReadPhase = currentReadPhase.getNextPhase();
            state = ReadState.FINISHED;
        }
        return state;
    }

    public WriteState write(SocketChannel outChannel)
        throws IllegalMessageStateException, IOException
    {
        WriteState state = WriteState.UNFINISHED;
        switch (currentWritePhase)
        {
            case HEADER:
                {
                    ByteBuffer headerBuffer = msgOut.getHeaderBuffer();
                    outChannel.write(headerBuffer);
                    if (!headerBuffer.hasRemaining())
                    {
                        currentWritePhase = currentWritePhase.getNextPhase();
                        state = writeData(outChannel, state);
                    }
                }
                break;
            case DATA:
                state = writeData(outChannel, state);
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
        if (state == WriteState.FINISHED)
        {
            currentWritePhase = Phase.HEADER;
            nextOutMessage();
        }
        return state;
    }

    private WriteState writeData(SocketChannel outChannel, WriteState stateRef)
        throws IOException, IllegalMessageStateException
    {
        WriteState state = stateRef;
        ByteBuffer dataBuffer = msgOut.getDataBuffer();
        outChannel.write(dataBuffer);
        if (!dataBuffer.hasRemaining())
        {
            // Finished sending the message
            state = WriteState.FINISHED;
            currentWritePhase = currentWritePhase.getNextPhase();
        }
        return state;
    }


    protected void addToQueue(Message msg)
    {
        try
        {
            // This method is single-threaded, no need to synchronize
            long msgSize = msg.getData().length;
            if (msgSize > msgRecvSizePeak)
            {
                msgRecvSizePeak = msgSize;
            }
        }
        catch (IllegalMessageStateException exc)
        {
            throw new ImplementationError(
                "Illegal message state, suspected error: Outbound message added to InQ",
                exc
            );
        }

        finishedMsgInQueue.add(msg);
        if (finishedMsgInQueue.size() >= MAX_INCOMING_QUEUE_SIZE)
        {
            /*
             * we reached MAX_INCOMING_QUEUE_SIZE. if we would allow
             * unlimited messages, the queue could get full which either
             * throw an out of memory error eventually or the queue.add will block.
             * Latter would block the whole tcpConnectorService-thread.
             *
             * Therefore we stop listening to OP_READ. This will be reverted
             * when a message which is ready to process is consumed (i.e.
             * leaves our queue).
             */
            disableInterestOps(OP_READ);
        }
    }

    @Override
    public ExtToolsManager getExtToolsManager()
    {
        return externalToolsManager;
    }

    @Override
    public StltConfig getStltConfig()
    {
        return stltConfig;
    }

    @Override
    public void setStltConfig(StltConfig stltConfigRef)
    {
        stltConfig = stltConfigRef;
    }

    @Override
    public void setDynamicProperties(List<Property> dynamicPropListRef)
    {
        synchronized (dynamicProperties)
        {
            dynamicProperties.clear();
            for (Property prop : dynamicPropListRef)
            {
                dynamicProperties.put(prop.getKey(), prop);
            }
        }
    }

    @Override
    public Property getDynamicProperty(String keyRef)
    {
        synchronized (dynamicProperties)
        {
            return dynamicProperties.get(keyRef);
        }
    }
}
