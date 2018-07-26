package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.WorkQueue;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class EventBroker
{
    private final WorkQueue workQueue;
    private final EventSender eventSender;
    private final ErrorReporter errorReporter;

    @Inject
    public EventBroker(
        @Named(LinStorModule.EVENT_WRITER_WORKER_POOL_NAME) WorkQueue workQueueRef,
        EventSender eventSenderRef,
        ErrorReporter errorReporterRef
    )
    {
        workQueue = workQueueRef;
        eventSender = eventSenderRef;
        errorReporter = errorReporterRef;
    }

    /**
     * Add a watch and send initial state for all relevant events.
     */
    public void createWatch(Watch watch)
    {
        errorReporter.logTrace("Submitting 'create watch' event: %s", watch.getEventIdentifier());
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'create watch' start: %s", watch.getEventIdentifier());
                eventSender.createWatch(watch);
                errorReporter.logTrace("Event 'create watch' end: %s", watch.getEventIdentifier());
            }
        );
    }

    public void deleteWatch(String peerId, int peerWatchId)
    {
        errorReporter.logTrace("Submitting 'delete watch' event");
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'delete watch' start");
                eventSender.deleteWatch(peerId, peerWatchId);
                errorReporter.logTrace("Event 'delete watch' end");
            }
        );
    }

    public void connectionClosed(Peer peer)
    {
        errorReporter.logTrace("Submitting 'connection closed' event: %s", peer);
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'connection closed' start: %s", peer);
                eventSender.connectionClosed(peer);
                errorReporter.logTrace("Event 'connection closed' end: %s", peer);
            }
        );
    }

    public void forwardEvent(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        switch (eventStreamAction)
        {
            case ApiConsts.EVENT_STREAM_VALUE:
                triggerEvent(eventIdentifier);
                break;
            case ApiConsts.EVENT_STREAM_OPEN:
                openEventStream(eventIdentifier);
                break;
            case ApiConsts.EVENT_STREAM_CLOSE_REMOVED:
                closeEventStream(eventIdentifier);
                break;
            case ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION:
                closeEventStream(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION, true);
                break;
            default:
                throw new ImplementationError("Unknown event action '" + eventStreamAction + "'");
        }
    }

    public void openEventStream(EventIdentifier eventIdentifier)
    {
        errorReporter.logTrace("Submitting 'open event stream' event: %s", eventIdentifier);
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'open event stream' start: %s", eventIdentifier);
                eventSender.openEventStream(eventIdentifier);
                errorReporter.logTrace("Event 'open event stream' end: %s", eventIdentifier);
            }
        );
    }

    public void openOrTriggerEvent(EventIdentifier eventIdentifier)
    {
        errorReporter.logTrace("Submitting 'open or trigger event' event: %s", eventIdentifier);
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'open or trigger event' start: %s", eventIdentifier);
                eventSender.openOrTriggerEvent(eventIdentifier);
                errorReporter.logTrace("Event 'open or trigger event' end: %s", eventIdentifier);
            }
        );
    }

    public void triggerEvent(EventIdentifier eventIdentifier)
    {
        errorReporter.logTrace("Submitting 'trigger event' event: %s", eventIdentifier);
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'trigger event' start: %s", eventIdentifier);
                eventSender.triggerEvent(eventIdentifier, ApiConsts.EVENT_STREAM_VALUE);
                errorReporter.logTrace("Event 'trigger event' end: %s", eventIdentifier);
            }
        );
    }

    public void closeEventStream(EventIdentifier eventIdentifier)
    {
        closeEventStream(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED, true);
    }

    public void closeEventStreamEvenIfNotOpen(EventIdentifier eventIdentifier)
    {
        closeEventStream(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED, false);
    }

    private void closeEventStream(EventIdentifier eventIdentifier, String eventStreamAction, boolean onlyIfOpen)
    {
        errorReporter.logTrace("Submitting 'close event stream' event: %s", eventIdentifier);
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'close event stream' start: %s", eventIdentifier);
                eventSender.closeEventStream(eventIdentifier, eventStreamAction, onlyIfOpen);
                errorReporter.logTrace("Event 'close event stream' end: %s", eventIdentifier);
            }
        );
    }

    public void closeAllEventStreams(String eventName, ObjectIdentifier objectIdentifier)
    {
        errorReporter.logTrace("Submitting 'close all event streams' event: %s %s", eventName, objectIdentifier);
        workQueue.submit(() ->
            {
                errorReporter.logTrace("Event 'close all event streams' start: %s %s", eventName, objectIdentifier);
                eventSender.closeAllEventStreams(eventName, objectIdentifier);
                errorReporter.logTrace("Event 'close all event streams' end: %s %s", eventName, objectIdentifier);
            }
        );
    }
}
