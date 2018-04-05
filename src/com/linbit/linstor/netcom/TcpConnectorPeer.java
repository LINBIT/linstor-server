package com.linbit.linstor.netcom;

import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectableChannel;

import java.nio.channels.SelectionKey;
import java.util.Deque;
import java.util.LinkedList;

import javax.net.ssl.SSLException;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Tracks the status of the communication with a peer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpConnectorPeer implements Peer
{
    private final Node node;

    private String peerId;

    private TcpConnector connector;

    // Current inbound message
    protected TcpConnectorMessage msgIn;

    // Current outbound message; cached for quicker access
    protected TcpConnectorMessage msgOut;

    // Queue of pending outbound messages
    // TODO: Put a capacity limit on the maximum number of queued outbound messages
    protected final Deque<TcpConnectorMessage> msgOutQueue;

    protected SelectionKey selKey;

    private AccessContext peerAccCtx;

    private Object attachment;

    protected boolean connected = false;
    protected boolean authenticated = false;
    protected boolean fullSyncFailed = false;

    // Volatile guarantees atomic read and write
    //
    // The counters are only incremented by only one thread
    // at a time, but may be concurrently read by multiple threads,
    // therefore requiring atomic read and write
    private volatile long msgSentCtr = 0;
    private volatile long msgRecvCtr = 0;

    protected long lastPingSent = -1;
    private long lastPongReceived = -1;

    protected Message internalPingMsg;
    protected Message internalPongMsg;

    private Map<ResourceName, ResourceState> resourceStateMap;

    private long fullSyncId;
    private final AtomicLong serializerId;
    private final ReadWriteLock serializerLock;

    protected TcpConnectorPeer(
        String peerIdRef,
        TcpConnector connectorRef,
        SelectionKey key,
        AccessContext accCtx,
        Node nodeRef
    )
    {
        peerId = peerIdRef;
        connector = connectorRef;
        node = nodeRef;
        msgOutQueue = new LinkedList<>();

        // Do not use createMessage() here!
        // The SslTcpConnectorPeer has no initialized SSLEngine instance yet,
        // so a NullPointerException would be thrown in createMessage().
        // After initialization of the sslEngine, msgIn will be overwritten with
        // a reference to a valid instance.
        msgIn = new TcpConnectorMessage(false);

        selKey = key;
        peerAccCtx = accCtx;
        attachment = null;

        internalPingMsg = new TcpHeaderOnlyMessage(MessageTypes.PING);
        internalPongMsg = new TcpHeaderOnlyMessage(MessageTypes.PONG);

        serializerId = new AtomicLong(0);
        serializerLock = new ReentrantReadWriteLock(true);
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

    protected TcpConnectorMessage createMessage(boolean forSend)
    {
        return new TcpConnectorMessage(forSend);
    }

    @Override
    public boolean sendMessage(Message msg)
        throws IllegalMessageStateException
    {
        boolean connFlag = connected;
        if (connFlag)
        {
            TcpConnectorMessage tcpConMsg;
            try
            {
                tcpConMsg = (TcpConnectorMessage) msg;
            }
            catch (ClassCastException ccExc)
            {
                tcpConMsg = createMessage(true);
                tcpConMsg.setData(msg.getData());
            }

            synchronized (this)
            {
                // Queue the message for sending
                if (msgOut == null)
                {
                    msgOut = tcpConMsg;
                }
                else
                {
                    msgOutQueue.add(tcpConMsg);
                }

                try
                {
                    // Outbound messages present, enable OP_WRITE
                    selKey.interestOps(OP_READ | OP_WRITE);
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
        closeConnection(false);
    }

    public void closeConnection(boolean allowReconnect)
    {
        connected = false;
        authenticated = false;
        connector.closeConnection(this, allowReconnect);
    }

    @Override
    public boolean isConnected()
    {
        return isConnected(true);
    }

    @Override
    public boolean isConnected(boolean ensureAuthenticated)
    {
        boolean ret = false;
        if (connected)
        {
            if (ensureAuthenticated)
            {
                if (authenticated)
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

    SelectionKey getSelectionKey()
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
                    selKey.interestOps(OP_READ);
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
    public void setResourceStates(final Map<ResourceName, ResourceState> resourceStateMapRef)
    {
        resourceStateMap = resourceStateMapRef;
    }

    @Override
    public Map<ResourceName, ResourceState> getResourceStates()
    {
        return resourceStateMap;
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
    public void fullSyncFailed()
    {
        fullSyncFailed = true;
        // just to be sure, that even if some component still sends an update, it should be
        // an invalid one. -1 will make it look like an out-dated update for the satellite.
        fullSyncId = -1;
    }

    @Override
    public boolean hasFullSyncFailed()
    {
        return fullSyncFailed;
    }
}
