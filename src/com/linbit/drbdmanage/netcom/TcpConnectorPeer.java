package com.linbit.drbdmanage.netcom;

import com.linbit.drbdmanage.security.AccessContext;
import java.nio.channels.SelectionKey;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import java.util.Deque;
import java.util.LinkedList;

/**
 * Tracks the status of the communication with a peer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpConnectorPeer implements Peer
{
    private TcpConnector connector;

    // Current inbound message
    TcpConnectorMessage msgIn;

    // Current outbound message; cached for quicker access
    TcpConnectorMessage msgOut;

    // Queue of pending outbound messages
    // TODO: Put a capacity limit on the maximum number of queued outbound messages
    private final Deque<TcpConnectorMessage> msgOutQueue;

    private SelectionKey selKey;

    private AccessContext peerAccCtx;

    private Object attachment;

    TcpConnectorPeer(TcpConnector connectorRef, SelectionKey key, AccessContext accCtx)
    {
        connector = connectorRef;
        msgOutQueue = new LinkedList<>();
        msgIn = new TcpConnectorMessage(false);
        selKey = key;
        peerAccCtx = accCtx;
    }

    @Override
    public Message createMessage()
    {
        return new TcpConnectorMessage(true);
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
            tcpConMsg = new TcpConnectorMessage(true);
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

            // Outbound messages present, enable OP_WRITE
            selKey.interestOps(OP_READ | OP_WRITE);
            connector.wakeup();
        }
    }

    void nextInMessage()
    {
        msgIn = new TcpConnectorMessage(false);
    }

    void nextOutMessage()
    {
        synchronized (this)
        {
            msgOut = msgOutQueue.pollFirst();
            if (msgOut == null)
            {
                // No more outbound messages present, disable OP_WRITE
                selKey.interestOps(OP_READ);
            }
        }
    }

    @Override
    public AccessContext getAccessContext()
    {
        return peerAccCtx;
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
}
