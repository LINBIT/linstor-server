package com.linbit.linstor.netcom;

import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectableChannel;

import java.nio.channels.SelectionKey;
import java.util.Deque;
import java.util.LinkedList;

import javax.net.ssl.SSLException;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import java.nio.channels.SocketChannel;

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
    public void sendMessage(Message msg)
        throws IllegalMessageStateException
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
            if (!connected)
            {
                throw new ImplementationError(new NotYetConnectedException());
            }

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
        connected = false;
        authenticated = false;
        connector.closeConnection(this);
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
    public void setAuthenticated(boolean authenticated)
    {
        this.authenticated = authenticated;
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

    void setSelectionKey(SelectionKey selKey)
    {
        this.selKey = selKey;
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
}
