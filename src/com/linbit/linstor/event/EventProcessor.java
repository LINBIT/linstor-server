package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Processes incoming events
 */
@Singleton
public class EventProcessor
{
    private final ErrorReporter errorReporter;
    private final Map<String, Provider<EventHandler>> eventHandlers;
    private final LinStorScope apiCallScope;

    // Synchronizes access to incomingEventStreamStore and pendingEventsPerPeer
    private final ReentrantLock eventHandlingLock;

    private final EventStreamStore incomingEventStreamStore;
    private final Map<String, EventBuffer> pendingEventsPerPeer;

    @Inject
    public EventProcessor(
        ErrorReporter errorReporterRef,
        Map<String, Provider<EventHandler>> eventHandlersRef,
        LinStorScope apiCallScopeRef
    )
    {
        errorReporter = errorReporterRef;
        eventHandlers = eventHandlersRef;
        apiCallScope = apiCallScopeRef;

        eventHandlingLock = new ReentrantLock();
        incomingEventStreamStore = new EventStreamStoreImpl();
        pendingEventsPerPeer = new HashMap<>();
    }

    public void outboundConnectionEstablished(Peer peer)
    {

        eventHandlingLock.lock();
        try
        {
            pendingEventsPerPeer.put(peer.getId(), new EventBuffer());
        }
        finally
        {
            eventHandlingLock.unlock();
        }
    }

    public void connectionClosed(Peer peer)
    {
        eventHandlingLock.lock();
        try
        {
            Node node = peer.getNode();
            if (node != null)
            {
                // The peer is a Satellite
                for (Map.Entry<String, Provider<EventHandler>> eventHandlerEntry : eventHandlers.entrySet())
                {
                    Collection<EventIdentifier> eventStreams =
                        incomingEventStreamStore.getDescendantEventStreams(new EventIdentifier(
                            eventHandlerEntry.getKey(),
                            node.getName(),
                            null,
                            null
                        ));

                    for (EventIdentifier eventIdentifier : eventStreams)
                    {
                        executeNoConnection(eventHandlerEntry.getValue(), eventIdentifier, peer);

                        incomingEventStreamStore.removeEventStream(eventIdentifier);
                    }
                }
            }

            pendingEventsPerPeer.remove(peer.getId());
        }
        finally
        {
            eventHandlingLock.unlock();
        }
    }

    public void handleEvent(
        long eventCounter,
        String eventAction,
        String eventName,
        String resourceNameStr,
        Integer volumeNr,
        Peer peer,
        InputStream eventDataIn
    )
    {
        eventHandlingLock.lock();
        try
        {
            EventBuffer eventBuffer = pendingEventsPerPeer.get(peer.getId());
            if (eventBuffer == null)
            {
                errorReporter.logWarning("Received event for unknown peer '" + peer.getId() + "'");
            }
            else
            {
                eventBuffer.addEvent(new Event(
                    eventCounter,
                    eventAction,
                    eventName,
                    resourceNameStr,
                    volumeNr,
                    peer,
                    eventDataIn
                ));

                executePendingEvents(eventBuffer);
            }
        }
        finally
        {
            eventHandlingLock.unlock();
        }
    }

    private void executePendingEvents(EventBuffer eventBuffer)
    {
        Event event = eventBuffer.getNextEvent();
        while (event != null)
        {
            try
            {
                Provider<EventHandler> eventHandlerProvider = eventHandlers.get(event.getEventName());
                if (eventHandlerProvider == null)
                {
                    errorReporter.logWarning("Unknown event '%s' received", event.getEventName());
                }
                else
                {
                    ResourceName resourceName =
                        event.getResourceNameStr() != null ? new ResourceName(event.getResourceNameStr()) : null;
                    VolumeNumber volumeNumber =
                        event.getVolumeNr() != null ? new VolumeNumber(event.getVolumeNr()) : null;

                    EventIdentifier eventIdentifier = new EventIdentifier(
                        event.getEventName(), event.getPeer().getNode().getName(), resourceName, volumeNumber);

                    String eventAction = event.getEventAction();

                    if (eventAction.equals(ApiConsts.EVENT_STREAM_OPEN))
                    {
                        incomingEventStreamStore.addEventStream(eventIdentifier);
                    }

                    eventHandlerProvider.get().execute(eventAction, eventIdentifier, event.getEventDataIn());

                    if (eventAction.equals(ApiConsts.EVENT_STREAM_CLOSE_REMOVED))
                    {
                        incomingEventStreamStore.removeEventStream(eventIdentifier);
                    }
                }
            }
            catch (InvalidNameException | ValueOutOfRangeException exc)
            {
                errorReporter.logWarning("Invalid event received: " + exc.getMessage());
            }
            catch (LinStorDataAlreadyExistsException exc)
            {
                errorReporter.logWarning(
                    "Ignoring open stream event for stream that is already open: " + exc.getMessage());
            }
            catch (Exception | ImplementationError exc)
            {
                errorReporter.reportError(exc);
            }

            event = eventBuffer.getNextEvent();
        }
    }

    private void executeNoConnection(
        Provider<EventHandler> eventHandler,
        EventIdentifier eventIdentifier,
        Peer peer
    )
    {
        apiCallScope.enter();
        try
        {
            apiCallScope.seed(Peer.class, peer);
            eventHandler.get().execute(
                ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION, eventIdentifier, null);
        }
        catch (IOException exc)
        {
            throw new ImplementationError(
                "Event handler for " + eventIdentifier + " failed on connection closed");
        }
        finally
        {
            apiCallScope.exit();
        }
    }

    private static class EventBuffer
    {
        private final PriorityQueue<Event> events =
            new PriorityQueue<>(Comparator.comparingLong(Event::getEventCounter));

        private long expectedEventCounter = 1;

        public void addEvent(Event event)
        {
            events.add(event);
        }

        public Event getNextEvent()
        {
            Event nextEvent = events.peek();
            boolean nextIsExpected = nextEvent != null && nextEvent.getEventCounter() == expectedEventCounter;
            if (nextIsExpected)
            {
                expectedEventCounter++;
            }
            return nextIsExpected ? events.poll() : null;
        }
    }

    private static class Event
    {
        private final long eventCounter;
        private final String eventAction;
        private final String eventName;
        private final String resourceNameStr;
        private final Integer volumeNr;
        private final Peer peer;
        private final InputStream eventDataIn;

        private Event(
            long eventCounterRef,
            String eventActionRef,
            String eventNameRef,
            String resourceNameStrRef,
            Integer volumeNrRef,
            Peer peerRef,
            InputStream eventDataInRef
        )
        {
            eventCounter = eventCounterRef;
            eventAction = eventActionRef;
            eventName = eventNameRef;
            resourceNameStr = resourceNameStrRef;
            volumeNr = volumeNrRef;
            peer = peerRef;
            eventDataIn = eventDataInRef;
        }

        public long getEventCounter()
        {
            return eventCounter;
        }

        public String getEventAction()
        {
            return eventAction;
        }

        public String getEventName()
        {
            return eventName;
        }

        public String getResourceNameStr()
        {
            return resourceNameStr;
        }

        public Integer getVolumeNr()
        {
            return volumeNr;
        }

        public Peer getPeer()
        {
            return peer;
        }

        public InputStream getEventDataIn()
        {
            return eventDataIn;
        }
    }
}
