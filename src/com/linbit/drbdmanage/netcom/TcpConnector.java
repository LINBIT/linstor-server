package com.linbit.drbdmanage.netcom;

/**
 * Server for TCP/IP connections to various other servers and clients
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface TcpConnector
{
    // TODO: Experimental; some means of adding a new connection that is to
    //       be created by the TcpConnector implementation
    public Peer connect();
}
