package com.linbit.linstor.netcom;

import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.api.protobuf.common.Ping;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import javax.net.ssl.SSLException;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Represents the peer of a connection
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Peer
{
    int MAX_INCOMING_QUEUE_SIZE = 1000;

    /**
     * Returns a unique identifier for this peer object
     *
     * @return Unique peer identifier
     */
    String getId();

    /**
     * Peer host address "ip:port"
     * @return peer host address if available.
     */
    @Nullable InetSocketAddress getHostAddr();

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
     * Wraps the parameter into a {@link Message} which is created by {@link #createMessage()} and
     * calls {@link #sendMessage(Message)}.
     *
     * @param data
     * @return
     */
    boolean sendMessage(byte[] data);

    /**
     * Wraps the parameter into a {@link Message} which is created by {@link #createMessage()} and
     * calls {@link #sendMessage(Message)}.
     * Additionally logs which Peer is the target and apiCall it uses
     *
     * @param data
     * @param apiCall
     *
     * @return
     */
    default boolean sendMessage(byte[] data, String apiCall)
    {
        return sendMessage(data);
    }

    /**
     * Get a zero-based sequence number for this peer.
     */
    long getNextIncomingMessageSeq();

    /**
     * Perform processing in a strictly ordered fashion.
     *
     * @param peerSeq A zero-based sequence number giving the order in which to process this call
     * @param publisher A publisher that will be subscribed to in strict order
     */
    void processInOrder(long peerSeq, Publisher<?> publisher);

    /**
     * Send an API call to this peer.
     *
     * @param apiCallName The API call to make
     * @param data The API call data
     * @return The stream of answers
     */
    Flux<ByteArrayInputStream> apiCall(String apiCallName, byte[] data);

    /**
     * Notify the peer that an API call answer has been received.
     */
    void apiCallAnswer(long apiCallId, ByteArrayInputStream data);

    /**
     * Notify the peer that an API call error has been received.
     */
    void apiCallError(long apiCallId, Throwable exc);

    /**
     * Notify the peer that the answers for an API call are complete.
     */
    void apiCallComplete(long apiCallId);

    /**
     * Sets the allowReconnect field of the peer. The peer itself does not necessarily have to do anything with this
     * field, just return it again in {@link #isAllowReconnect()}.
     *
     * It is recommended to first call "<code>setAllowReconnect(false)</code>" before tearing down the connection to
     * avoid race conditions with controller's PingTask / ReconnectTask.
     */
    void setAllowReconnect(boolean allowReconnectRef);

    /**
     * Returns the value set via {@link #setAllowReconnect(boolean)} and should indicate if this peer instance should
     * be used for a reconnect-attempt. <code>True</code> by default, unless {@link #setAllowReconnect(boolean)} was
     * called.
     */
    boolean isAllowReconnect();

    /**
     * Closes the connection to the peer
     */
    void closeConnection();

    /**
     * Closes the connection to the peer
     */
    void closeConnection(boolean allowReconnect);

    /**
     * Notifies the peer that the underlying connection is closing.
     */
    void connectionClosing();

    /**
     * This is the same as calling {@code isConnected(true)}
     */
    boolean isOnline();

    ApiConsts.ConnectionStatus getConnectionStatus();

    void setConnectionStatus(ApiConsts.ConnectionStatus status);

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
     * Returns the size of the biggest message that was sent to the peer
     *
     * @return Size of the biggest message sent, in bytes
     */
    long msgSentMaxSize();


    /**
     * Returns the size of the biggest message that was received from the peer
     *
     * @return Size of the biggest message received, in bytes
     */
    long msgRecvMaxSize();

    /**
     * Returns the destination internet address of the peer connection
     *
     * @return Internet address of the peer
     */
    @Nullable
    InetSocketAddress peerAddress();

    /**
     * Returns the internet address of the local connector
     *
     * @return Internet address of the peer
     */
    InetSocketAddress localAddress();

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
     * Read lock required when accessing the SatelliteState; write lock when modifying.
     */
    ReadWriteLock getSatelliteStateLock();

    /**
     * Get the state data for this satellite, if the peer represents one.
     *
     * The locks from {@link #getSatelliteState()}} synchronize access to the data.
     */
    @Nullable
    SatelliteState getSatelliteState();

    /**
     * Whenever a FullSync or a LinStor object gets serialized, the FullSync timestamp
     * and / or the serializer-ID might change. Use this lock for (possibly) concurrent
     * modification for those.
     * @return
     */
    ReadWriteLock getSerializerLock();

    /**
     * Sets the fullSyncId for serialization.
     *
     * It is advised to grab the write lock of {@link #getSerializerLock()} prior this call
     *
     * @param timestamp
     */
    void setFullSyncId(long timestamp);

    /**
     * Returns the current fullSyncId.
     *
     * It is advised to grab the read lock of {@link #getSerializerLock()} prior this call
     *
     * @return
     */
    long getFullSyncId();

    /**
     * Returns the next serializer Id. This method should be called when serializing
     * any LinStor object for the satellite
     *
     * It is advised to grab the read lock of {@link #getSerializerLock()} prior this call.
     *
     * @return
     */
    long getNextSerializerId();

    /**
     * @see #fullSyncFailed(com.linbit.linstor.api.ApiConsts.ConnectionStatus)
     */
    default void fullSyncFailed()
    {
        fullSyncFailed(null);
    }
    /**
     * The satellite failed to apply our fullSync. This method should set internal flags
     * to prevent sending any further updates or fullSyncs to the satellite, as those will most
     * likely also cause the same exception on the satellite.
     * However, not all communication should be prevented to the satellite.
     * E.g. Ping/Pong and client-proxy messages should still work / forwarded.
     *
     * @param suggestedConnStatusRef the connection status. If null, ApiConsts.ConnectionStatus.FULL_SYNC_FAILED is
     *     used
     */
    void fullSyncFailed(@Nullable ApiConsts.ConnectionStatus suggestedConnStatusRef);

    /**
     * Returns true if the method {@link #fullSyncFailed()} was already called, false otherwise.
     *
     * @return
     */
    boolean hasFullSyncFailed();

    /**
     * The satellite successfully applied our fullSync. This method should set internal flags
     * to allow sending messages that require an applied fullSync.
     */
    void fullSyncApplied();

    /**
     * Returns true if the method {@link #fullSyncApplied()} was already called, false otherwise.
     *
     * @return
     */
    boolean isFullSyncApplied();

    /**
     * Returns true if the peer has a complete Message object ready to be processed.
     *
     * @return
     */
    boolean hasNextMsgIn();

    /**
     * Returns a complete {@link Message} object for further processing. May return null
     * if {@link #hasNextMsgIn()} returns false or throw an {@link ImplementationError}.
     * @return
     */
    Message nextCurrentMsgIn();

    ExtToolsManager getExtToolsManager();

    StltConfig getStltConfig();

    void setStltConfig(StltConfig stltConfig);

    void setDynamicProperties(List<Property> dynamicPropListRef);

    /**
     * Returns null if the given key has no registered dynamic property on the satellite.
     * That might be the case for non Satellite-peer but also for Satellites that do not have
     * DRBD installed
     */
    Property getDynamicProperty(String keyRef);
}
