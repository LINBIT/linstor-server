package com.linbit.drbdmanage.netcom;

import com.linbit.drbdmanage.security.AccessContext;

/**
 * Represents the peer of a connection
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Peer
{
    /**
     * Returns the security context of the connected peer
     *
     * @return AccessContext object representing the connected peer's security context
     */
    AccessContext getAccessContext();

    /**
     * Attaches the object to the peer
     *
     * @param attachment The object to attach to the peer
     */
    void attach(Object attachment);

    /**
     * Fetches the object attached to the peer
     *
     * @return The object attached to the peer
     */
    Object getAttachment();

    /**
     * Queues a message for sending to the peer
     *
     * @param msg Message to send
     * @throws IllegalMessageStateException If the message object is not in a valid state for sending
     */
    void sendMessage(Message msg) throws IllegalMessageStateException;
}
