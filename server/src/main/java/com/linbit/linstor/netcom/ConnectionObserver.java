package com.linbit.linstor.netcom;

import java.io.IOException;

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
     * @throws IOException
     */
    void outboundConnectionEstablished(Peer connPeer) throws IOException;

    /**
     * Called when an outbound connection is establishing but not yet finished
     *
     * @param peerRef
     * @throws IOException
     */
    void outboundConnectionEstablishing(Peer peerRef) throws IOException;

    /**
     * Called when an inbound connection is established
     * @param connPeer The connected peer
     */
    void inboundConnectionEstablished(Peer connPeer);

    /**
     * Called when a connection to a peer is closed
     *
     * @param connPeer The peer that was disconnected
     * @param shuttingDown True if the connections being generally shut down.
     */
    void connectionClosed(Peer connPeer, boolean shuttingDown);

}
