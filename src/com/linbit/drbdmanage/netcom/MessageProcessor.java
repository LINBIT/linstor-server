package com.linbit.drbdmanage.netcom;

/**
 * Interface to the system component that processes messages that have been received
 * by the server
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface MessageProcessor
{
    public void processMessage(Message msg, TcpConnector connector, Peer client);
}
