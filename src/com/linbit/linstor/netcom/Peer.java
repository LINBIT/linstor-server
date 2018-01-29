package com.linbit.linstor.netcom;

import com.linbit.ServiceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.protobuf.common.Ping;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import javax.net.ssl.SSLException;

/**
 * Represents the peer of a connection
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Peer
{
    /**
     * Returns a unique identifier for this peer object
     *
     * @return Unique peer identifier
     */
    String getId();

    /**
     * Returns the {@link Node} object the peer represents
     *
     * @return Node instance
     */
    Node getNode();

    /**
     * Returns the service instance name of the connector associated with this peer object
     *
     * @return Connector service instance name
     */
    ServiceName getConnectorInstanceName();

    /**
     * Returns the security context of the connected peer
     *
     * @return AccessContext object representing the connected peer's security context
     */
    AccessContext getAccessContext();

    /**
     * Sets a new access context for the connected peer
     *
     * @param privilegedCtx The access context of the subject changing the peer's access context
     * @param newAccCtx The new access context to associate with the peer
     */
    void setAccessContext(AccessContext privilegedCtx, AccessContext newAccCtx)
        throws AccessDeniedException;

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
     * Creates a new message for sending to this peer
     *
     * @return New message instance
     */
    Message createMessage();

    /**
     * Queues a message for sending to the peer
     *
     * @param msg Message to send
     * @throws IllegalMessageStateException If the message object is not in a valid state for sending
     */
    boolean sendMessage(Message msg) throws IllegalMessageStateException;

    /**
     * Closes the connection to the peer
     *
     * FIXME: This said "throws SSLException", but JavaDoc complains. Check whether it actually throw SSLException
     *        and whether it is even a good idea to do that.
     */
    void closeConnection();

    /**
     * This is the same as calling {@code isConnected(true)}
     */
    boolean isConnected();

    /**
     * Returns false if no connection is established yet.
     * Returns false if connection is established but not authenticated and {@code ensureAuthenticated} is true.
     * Returns true otherwise.
     */
    boolean isConnected(boolean ensureAuthenticated);

    boolean isAuthenticated();

    void setAuthenticated(boolean authenticated);

    /**
     * Returns the capacity of the queue for outbound messages
     *
     * @return Capacity of the outbound messages queue
     */
    int outQueueCapacity();

    /**
     * Returns the number of currently queued outbound messages
     *
     * @return Number of currently queued outbound messages
     */
    int outQueueCount();

    /**
     * Returns the number of messages that were sent to the peer
     *
     * @return Number of messages that were sent
     */
    long msgSentCount();

    /**
     * Returns the number of messages that were received from the peer
     *
     * @return Number of messages that were received
     */
    long msgRecvCount();

    /**
     * Returns the destination internet address of the peer connection
     *
     * @return Internet address of the peer
     */
    InetSocketAddress peerAddress();

    /**
     * Called when the connection is established
     * @throws SSLException
     */
    void connectionEstablished() throws SSLException;

    /**
     * Waits until someone calls the {@link Peer#connectionEstablished()} method
     * @throws InterruptedException
     */
    void waitUntilConnectionEstablished() throws InterruptedException;

    /**
     * Returns the {@link TcpConnector} handling this peer
     *
     * @return
     */
    TcpConnector getConnector();

    /**
     * Sends an internal ping packet (no data, MessageType = {@link MessageTypes#PING}
     */
    void sendPing();

    /**
     * Sends an internal pong packet (no data, MessageType = {@link MessageTypes#PONG}
     */
    void sendPong();
    /**
     * This method should only be called by {@link Ping}, in order to calculate the latency
     */
    void pongReceived();

    /**
     * Returns a timestamp in milliseconds when the last ping message was sent
     * (e.g. {@link Peer#sendPing()} was called)
     *
     * @return
     */
    long getLastPingSent();

    /**
     * Returns a timestamp in milliseconds when the last ping message was received
     * (e.g. {@link Peer#pongReceived()} was called)
     *
     * @return
     */
    long getLastPongReceived();

    /**
     * Sets the current resource states for this peer(satellite).
     * @param resourceStateMap new resource states.
     */
    void setResourceStates(final Map<ResourceName, ResourceState> resourceStateMap);

    /**
     * Method to access the current resource states;
     * @return A map containing the currently known resource states
     */
    Map<ResourceName, ResourceState> getResourceStates();

    /**
     * Whenever a FullSync or a LinStor object gets serialized, the FullSync timestamp
     * and / or the serializer-ID might change. Use this lock for (possibly) concurrent
     * modification for those.
     * @return
     */
    ReadWriteLock getSerializerLock();

    /**
     * Sets the timestamp when a new FullSync is serialized.
     *
     * It is advised to grab the write lock of {@link #getSerializerLock()} prior this call
     *
     * @param timestamp
     */
    void setFullSyncTimestamp(long timestamp);

    /**
     * Returns the timestamp of the last FullSync serialization
     *
     * It is advised to grab the read lock of {@link #getSerializerLock()} prior this call
     *
     * @return
     */
    long getFullSyncTimestamp();

    /**
     * Returns the next serializer Id. This method should be called when serializing
     * any LinStor object for the satellite
     *
     * It is advised to grab the read lock of {@link #getSerializerLock()} prior this call.
     *
     * @return
     */
    long getNextSerializerId();
}
