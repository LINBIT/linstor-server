package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.WorkQueue;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class EventBroker
{
    private final WorkQueue workQueue;
    private final EventSender eventSender;

    @Inject
    public EventBroker(
        @Named(LinStorModule.EVENT_WRITER_WORKER_POOL_NAME) WorkQueue workQueueRef,
        EventSender eventSenderRef
    )
    {
        workQueue = workQueueRef;
        eventSender = eventSenderRef;
    }

    /**
     * Add a watch and send initial state for all relevant events.
     */
    public void createWatch(Watch watch)
    {
        workQueue.submit(() -> eventSender.createWatch(watch));
    }

    public void connectionClosed(Peer peer)
    {
        workQueue.submit(() -> eventSender.connectionClosed(peer));
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
                closeEventStream(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION);
                break;
            default:
                throw new ImplementationError("Unknown event action '" + eventStreamAction + "'");
        }
    }

    public void openEventStream(EventIdentifier eventIdentifier)
    {
        workQueue.submit(() -> eventSender.openEventStream(eventIdentifier));
    }

    public void openOrTriggerEvent(EventIdentifier eventIdentifier)
    {
        workQueue.submit(() -> eventSender.openOrTriggerEvent(eventIdentifier));
    }

    public void triggerEvent(EventIdentifier eventIdentifier)
    {
        workQueue.submit(() -> eventSender.triggerEvent(eventIdentifier, ApiConsts.EVENT_STREAM_VALUE));
    }

    public void closeEventStream(EventIdentifier eventIdentifier)
    {
        closeEventStream(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED);
    }

    public void closeEventStreamWithData(EventIdentifier eventIdentifier, byte[] eventData)
    {
        closeEventStream(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED, eventData);
    }

    private void closeEventStream(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        workQueue.submit(() -> eventSender.closeEventStream(eventIdentifier, eventStreamAction));
    }

    private void closeEventStream(EventIdentifier eventIdentifier, String eventStreamAction, byte[] eventData)
    {
        workQueue.submit(() -> eventSender.closeEventStream(eventIdentifier, eventStreamAction, eventData));
    }

    public void closeAllEventStreams(String eventName, ObjectIdentifier objectIdentifier)
    {
        workQueue.submit(() -> eventSender.closeAllEventStreams(eventName, objectIdentifier));
    }

    public byte[] getEventData(EventIdentifier eventIdentifier)
    {
        return eventSender.getEventData(eventIdentifier);
    }
}
