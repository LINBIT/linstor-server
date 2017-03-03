package com.linbit.drbdmanage.netcom;

import java.util.Deque;
import java.util.LinkedList;

/**
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

    TcpConnectorPeer(TcpConnector connectorRef)
    {
        connector = connectorRef;
        msgOutQueue = new LinkedList<>();
        msgIn = new TcpConnectorMessage(false);
    }

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

        synchronized (msgOutQueue)
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
        }

        // TODO: Inform the connector about the outbound message
    }

    void nextInMessage()
    {
        msgIn = new TcpConnectorMessage(false);
    }

    void nextOutMessage()
    {
        synchronized (msgOutQueue)
        {
            msgOut = msgOutQueue.pollFirst();
        }
    }
}
