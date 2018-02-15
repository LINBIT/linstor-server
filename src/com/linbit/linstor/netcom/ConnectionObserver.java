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
     * Called when an inbound connection is established
     * @param connPeer The connected peer
     */
    void inboundConnectionEstablished(Peer connPeer);

    /**
     * Called when a connection to a peer is closed
     *
     * @param connPeer The peer that was disconnected
     * @param allowReconnect If true, the reconnector task will try to reconnect.
     */
    void connectionClosed(Peer connPeer, boolean allowReconnect);
}
