package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import reactor.core.publisher.Flux;

/**
 * Processes incoming events
 */
@Singleton
public class EventProcessor
{
    private final ErrorReporter errorReporter;
    private final Map<String, Provider<EventHandler>> eventHandlers;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<Peer> peerProvider;

    // Synchronizes access to incomingEventStreamStore and pendingEventsPerPeer
    private final ReentrantLock eventHandlingLock;

    private final EventStreamStore incomingEventStreamStore;

    @Inject
    public EventProcessor(
        ErrorReporter errorReporterRef,
        Map<String, Provider<EventHandler>> eventHandlersRef,
        LockGuardFactory lockGuardFactoryRef,
        Provider<Peer> peerProviderRef
    )
    {
        errorReporter = errorReporterRef;
        eventHandlers = eventHandlersRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerProvider = peerProviderRef;

        eventHandlingLock = new ReentrantLock();
        incomingEventStreamStore = new EventStreamStoreImpl();
    }

    public void connectionClosed(Peer peer)
    {
        eventHandlingLock.lock();
        try
        {
            Node node = peer.getNode();
            if (node != null && !node.isDeleted())
            {
                // The peer is a Satellite
                for (Map.Entry<String, Provider<EventHandler>> eventHandlerEntry : eventHandlers.entrySet())
                {
                    Collection<EventIdentifier> eventStreams = incomingEventStreamStore.getDescendantEventStreams(
                        EventIdentifier.node(eventHandlerEntry.getKey(), node.getName())
                    );

                    for (EventIdentifier eventIdentifier : eventStreams)
                    {
                        executeNoConnection(eventHandlerEntry.getValue(), eventIdentifier);

                        incomingEventStreamStore.removeEventStream(eventIdentifier);
                    }
                }
            }
        }
        finally
        {
            eventHandlingLock.unlock();
        }
    }

    public Flux<?> handleEvent(
        String eventAction,
        String eventName,
        String resourceNameStr,
        Integer volumeNr,
        String snapshotNameStr,
        String peerNodeNameStr,
        InputStream eventDataIn
    )
    {
        try (LockGuard lockGuard = lockGuardFactory.build(LockGuardFactory.LockType.WRITE, NODES_MAP))
        {
            eventHandlingLock.lock();
            try
            {
                Provider<EventHandler> eventHandlerProvider = eventHandlers.get(eventName);
                if (eventHandlerProvider == null)
                {
                    errorReporter.logWarning("Unknown event '%s' received", eventName);
                }
                else
                {
                    ResourceName resourceName =
                        resourceNameStr != null ? new ResourceName(resourceNameStr) : null;
                    VolumeNumber volumeNumber =
                        volumeNr != null ? new VolumeNumber(volumeNr) : null;
                    SnapshotName snapshotName =
                        snapshotNameStr != null ? new SnapshotName(snapshotNameStr) : null;
                    NodeName peerNodeName = peerNodeNameStr != null ? new NodeName(peerNodeNameStr) : null;

                    Peer peer = peerProvider.get();
                    EventIdentifier eventIdentifier = new EventIdentifier(
                        eventName,
                        new ObjectIdentifier(
                            peer.getNode().getName(),
                            resourceName,
                            volumeNumber,
                            snapshotName,
                            peerNodeName
                        )
                    );

                    incomingEventStreamStore.addEventStreamIfNew(eventIdentifier);

                    errorReporter.logTrace("Peer %s, event '%s' %s", peer, eventIdentifier, eventAction);
                    eventHandlerProvider.get().execute(eventAction, eventIdentifier, eventDataIn);

                    if (eventAction.equals(InternalApiConsts.EVENT_STREAM_CLOSE_REMOVED))
                    {
                        incomingEventStreamStore.removeEventStream(eventIdentifier);
                    }
                }
            }
            catch (InvalidNameException | ValueOutOfRangeException exc)
            {
                errorReporter.logWarning("Invalid event received: " + exc.getMessage());
            }
            catch (Exception | ImplementationError exc)
            {
                errorReporter.reportError(exc);
            }
            finally
            {
                eventHandlingLock.unlock();
            }
        }
        return Flux.empty();
    }

    private void executeNoConnection(
        Provider<EventHandler> eventHandler,
        EventIdentifier eventIdentifier
    )
    {
        try
        {
            eventHandler.get().execute(
                InternalApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION, eventIdentifier, null);
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc, null, null,
                "Event handler for " + eventIdentifier + " failed on connection closed");
        }
    }
}
