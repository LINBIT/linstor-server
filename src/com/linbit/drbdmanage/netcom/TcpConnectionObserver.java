package com.linbit.drbdmanage.netcom;

/**
 *
 * @author raltnoeder
 */
public interface TcpConnectionObserver
{
    void outboundConnectionEstablished(Peer connPeer);

    void inboundConnectionEstablished(Peer connPeer);

    void connectionClosed(Peer connPeer);
}
