package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.WorkQueue;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class EventBroker
{
    private final ErrorReporter errorReporter;
    private final WatchStore watchStore;
    private final EventStreamStore eventStreamStore;
    private final CommonSerializer commonSerializer;
    private final WorkQueue workQueue;
    private final CoreModule.PeerMap peerMap;
    private final Map<String, EventWriter> eventWriters;

    private final ReentrantLock watchAndStreamLock;

    @Inject
    public EventBroker(
        ErrorReporter errorReporterRef,
        WatchStore watchStoreRef,
        EventStreamStore eventStreamStoreRef,
        CommonSerializer commonSerializerRef,
        @Named(LinStorModule.EVENT_WRITER_WORKER_POOL_NAME) WorkQueue workQueueRef,
        CoreModule.PeerMap peerMapRef,
        Map<String, EventWriter> eventWritersRef
    )
    {
        errorReporter = errorReporterRef;
        watchStore = watchStoreRef;
        eventStreamStore = eventStreamStoreRef;
        commonSerializer = commonSerializerRef;
        workQueue = workQueueRef;
        peerMap = peerMapRef;
        eventWriters = eventWritersRef;

        watchAndStreamLock = new ReentrantLock();
    }

    /**
     * Add a watch and send initial state for all relevant events.
     */
    public void createWatch(Watch watch)
        throws LinStorDataAlreadyExistsException
    {
        watchAndStreamLock.lock();
        try
        {
            Collection<String> eventNames = getMatchingEventNames(watch.getEventIdentifier().getEventName());

            for (String eventName : eventNames)
            {
                Collection<EventIdentifier> eventStreams =
                    eventStreamStore.getDescendantEventStreams(new EventIdentifier(
                        eventName,
                        watch.getEventIdentifier().getNodeName(),
                        watch.getEventIdentifier().getResourceName(),
                        watch.getEventIdentifier().getVolumeNumber()
                    ));

                for (EventIdentifier eventIdentifier : eventStreams)
                {
                    EventWriter eventWriter = eventWriters.get(eventIdentifier.getEventName());
                    if (eventWriter == null)
                    {
                        errorReporter.logWarning("Stream is open for unknown event " + eventIdentifier.getEventName());
                    }
                    else
                    {
                        writeAndSend(
                            eventIdentifier,
                            ApiConsts.EVENT_STREAM_OPEN,
                            eventWriter,
                            Collections.singleton(watch)
                        );
                    }
                }
            }

            watchStore.addWatch(watch);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    private Collection<String> getMatchingEventNames(String eventName)
    {
        return eventName == null || eventName.isEmpty() ? eventWriters.keySet() : Collections.singleton(eventName);
    }

    public void connectionClosed(Peer peer)
    {
        watchAndStreamLock.lock();
        try
        {
            Node node = peer.getNode();
            if (node != null)
            {
                // The peer is a Satellite
                for (String eventName : eventWriters.keySet())
                {
                    Collection<EventIdentifier> eventStreams =
                        eventStreamStore.getDescendantEventStreams(new EventIdentifier(
                            eventName,
                            node.getName(),
                            null,
                            null
                        ));

                    for (EventIdentifier eventIdentifier : eventStreams)
                    {
                        Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

                        writeAndSend(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION, null, watches);

                        eventStreamStore.removeEventStream(eventIdentifier);
                    }
                }
            }

            watchStore.removeWatchesForPeer(peer.getId());
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
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
                throw new ImplementationError("NoConnection event received from satellite");
            default:
                throw new ImplementationError("Unknown event action '" + eventStreamAction + "'");
        }
    }

    public void openEventStream(EventIdentifier eventIdentifier)
    {
        watchAndStreamLock.lock();
        try
        {
            eventStreamStore.addEventStream(eventIdentifier);
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ImplementationError(
                "Stream is already open: " + eventIdentifier, exc);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
        triggerEvent(eventIdentifier, ApiConsts.EVENT_STREAM_OPEN);
    }

    public void openOrTriggerEvent(EventIdentifier eventIdentifier)
    {
        boolean isNew;
        watchAndStreamLock.lock();
        try
        {
            isNew = eventStreamStore.addEventStreamIfNew(eventIdentifier);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
        triggerEvent(eventIdentifier, isNew ? ApiConsts.EVENT_STREAM_OPEN : ApiConsts.EVENT_STREAM_VALUE);
    }

    public void triggerEvent(EventIdentifier eventIdentifier)
    {
        triggerEvent(eventIdentifier, ApiConsts.EVENT_STREAM_VALUE);
    }

    private void triggerEvent(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        EventWriter eventWriter = eventWriters.get(eventIdentifier.getEventName());
        if (eventWriter == null)
        {
            errorReporter.logError("Cannot trigger unknown event '%s'", eventIdentifier.getEventName());
        }
        else
        {
            Collection<Watch> watches;

            watchAndStreamLock.lock();
            try
            {
                watches = watchStore.getWatchesForEvent(eventIdentifier);
            }
            finally
            {
                watchAndStreamLock.unlock();
            }

            if (!watches.isEmpty())
            {
                writeAndSend(eventIdentifier, eventStreamAction, eventWriter, watches);
            }
        }
    }

    public void closeEventStream(EventIdentifier eventIdentifier)
    {
        watchAndStreamLock.lock();
        try
        {
            Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

            writeAndSend(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED, null, watches);

            eventStreamStore.removeEventStream(eventIdentifier);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void closeAllEventStreams(String eventName, ObjectIdentifier objectIdentifier)
    {
        watchAndStreamLock.lock();
        try
        {
            Collection<EventIdentifier> eventStreams =
                eventStreamStore.getDescendantEventStreams(new EventIdentifier(
                    eventName,
                    objectIdentifier.getNodeName(),
                    objectIdentifier.getResourceName(),
                    objectIdentifier.getVolumeNumber()
                ));

            for (EventIdentifier eventIdentifier : eventStreams)
            {
                Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

                writeAndSend(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED, null, watches);

                eventStreamStore.removeEventStream(eventIdentifier);
            }
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    private void writeAndSend(
        EventIdentifier eventIdentifier,
        String eventStreamAction,
        EventWriter eventWriter,
        Collection<Watch> watches
    )
    {
        workQueue.submit(new EventSender(eventIdentifier, eventStreamAction, eventWriter, watches));
    }

    private class EventSender implements Runnable
    {
        private final EventIdentifier eventIdentifier;
        private final String eventStreamAction;
        private final EventWriter eventWriter;
        private final Collection<Watch> watches;

        EventSender(
            EventIdentifier eventIdentifierRef,
            String eventStreamActionRef,
            EventWriter eventWriterRef,
            Collection<Watch> watchesRef
        )
        {
            eventIdentifier = eventIdentifierRef;
            eventStreamAction = eventStreamActionRef;
            eventWriter = eventWriterRef;
            watches = watchesRef;
        }

        @Override
        public void run()
        {
            try
            {
                byte[] eventData = eventWriter == null ? new byte[] {} :
                    eventWriter.writeEvent(eventIdentifier.getObjectIdentifier());

                if (eventData != null)
                {
                    watches.forEach(watch -> sendEvent(watch, eventData));
                }
            }
            catch (Exception exc)
            {
                errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "Failed to write event " + eventIdentifier.getEventName()
                );
            }
        }

        private void sendEvent(
            Watch watch,
            byte[] eventData
        )
        {
            Peer peer;
            synchronized (peerMap)
            {
                peer = peerMap.get(watch.getPeerId());
            }

            if (peer == null)
            {
                errorReporter.logWarning("Watch for unknown peer %s", watch.getPeerId());
            }
            else
            {
                byte[] eventHeaderBytes = commonSerializer.builder(ApiConsts.API_EVENT)
                    .event(watch.getPeerWatchId(), eventIdentifier, eventStreamAction)
                    .build();

                byte[] completeData = new byte[eventHeaderBytes.length + eventData.length];
                System.arraycopy(eventHeaderBytes, 0, completeData, 0, eventHeaderBytes.length);
                System.arraycopy(eventData, 0, completeData, eventHeaderBytes.length, eventData.length);

                peer.sendMessage(completeData);
            }
        }

    }
}
