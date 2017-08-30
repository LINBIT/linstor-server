package com.linbit.drbdmanage.netcom;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.linbit.SystemService;

/**
 * Server for TCP/IP connections to various other servers and clients
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface TcpConnector extends SystemService
{
    // TODO: Experimental; some means of adding a new connection that is to
    //       be created by the TcpConnector implementation
    Peer connect(InetSocketAddress address) throws IOException;

    void reconnect(Peer peer) throws IOException;

    void closeConnection(TcpConnectorPeer peerObj);

    /**
     * Wakes up the connector's selector
     */
    void wakeup();
}
