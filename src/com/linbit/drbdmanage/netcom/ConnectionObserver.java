package com.linbit.drbdmanage.netcom;

/**
 * Interface for classes that track the connection status of peers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ConnectionObserver
{
    /**
     * Called when an outbound connection is established
     *
     * @param connPeer The connected peer
     */
    void outboundConnectionEstablished(Peer connPeer);

    /**
     * Called when an inbound connection is established
     * @param connPeer The connected peer
     */
    void inboundConnectionEstablished(Peer connPeer);

    /**
     * Called when a connection to a peer is closed
     *
     * @param connPeer The peer that was disconnected
     */
    void connectionClosed(Peer connPeer);
}
