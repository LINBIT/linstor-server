package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class EventSender
{
    private final ErrorReporter errorReporter;
    private final WatchStore watchStore;
    private final CommonSerializer commonSerializer;
    private final CoreModule.PeerMap peerMap;
    private final Map<String, EventWriter> eventWriters;

    // Serialize event writing and sending.
    // This could be relaxed to allow events to be written on multiple threads, if required for performance.
    private final ReentrantLock watchAndStreamLock;
    private final EventStreamStore outgoingEventStreamStore;

    @Inject
    public EventSender(
        ErrorReporter errorReporterRef,
        WatchStore watchStoreRef,
        CommonSerializer commonSerializerRef,
        CoreModule.PeerMap peerMapRef,
        Map<String, EventWriter> eventWritersRef
    )
    {
        errorReporter = errorReporterRef;
        watchStore = watchStoreRef;
        commonSerializer = commonSerializerRef;
        peerMap = peerMapRef;
        eventWriters = eventWritersRef;

        watchAndStreamLock = new ReentrantLock();
        outgoingEventStreamStore = new EventStreamStoreImpl();
    }

    public void createWatch(Watch watch)
    {
        watchAndStreamLock.lock();
        try
        {
            watchStore.addWatch(watch);

            Collection<String> eventNames = getMatchingEventNames(watch.getEventIdentifier().getEventName());

            for (String eventName : eventNames)
            {
                Collection<EventIdentifier> eventStreams =
                    outgoingEventStreamStore.getDescendantEventStreams(new EventIdentifier(
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
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            errorReporter.logError(
                "Watch already exists for peer " + watch.getPeerId() + ", id " + watch.getPeerWatchId());
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void connectionClosed(Peer peer)
    {
        watchAndStreamLock.lock();
        try
        {
            watchStore.removeWatchesForPeer(peer.getId());
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void openEventStream(EventIdentifier eventIdentifier)
    {
        watchAndStreamLock.lock();
        try
        {
            outgoingEventStreamStore.addEventStream(eventIdentifier);
            sendToWatchers(eventIdentifier, ApiConsts.EVENT_STREAM_OPEN);
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
    }

    public void openOrTriggerEvent(EventIdentifier eventIdentifier)
    {
        boolean isNew;
        watchAndStreamLock.lock();
        try
        {
            isNew = outgoingEventStreamStore.addEventStreamIfNew(eventIdentifier);
            sendToWatchers(eventIdentifier, isNew ? ApiConsts.EVENT_STREAM_OPEN : ApiConsts.EVENT_STREAM_VALUE);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void triggerEvent(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        watchAndStreamLock.lock();
        try
        {
            sendToWatchers(eventIdentifier, eventStreamAction);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public byte[] getEventData(EventIdentifier eventIdentifier)
    {
        return writeEventData(eventIdentifier, eventWriters.get(eventIdentifier.getEventName()));
    }

    public void closeEventStream(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        watchAndStreamLock.lock();
        try
        {
            EventWriter eventWriter = eventWriters.get(eventIdentifier.getEventName());
            Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

            writeAndSend(eventIdentifier, eventStreamAction, eventWriter, watches);

            outgoingEventStreamStore.removeEventStream(eventIdentifier);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void closeEventStream(EventIdentifier eventIdentifier, String eventStreamAction, byte[] eventData)
    {
        watchAndStreamLock.lock();
        try
        {
            Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

            writeAndSendWithEventData(eventIdentifier, eventStreamAction, eventData, watches);

            outgoingEventStreamStore.removeEventStream(eventIdentifier);
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
                outgoingEventStreamStore.getDescendantEventStreams(new EventIdentifier(
                    eventName,
                    objectIdentifier.getNodeName(),
                    objectIdentifier.getResourceName(),
                    objectIdentifier.getVolumeNumber()
                ));

            for (EventIdentifier eventIdentifier : eventStreams)
            {
                Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

                writeAndSend(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED, null, watches);

                outgoingEventStreamStore.removeEventStream(eventIdentifier);
            }
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    private void sendToWatchers(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        EventWriter eventWriter = eventWriters.get(eventIdentifier.getEventName());
        if (eventWriter == null)
        {
            errorReporter.logError("Cannot trigger unknown event '%s'", eventIdentifier.getEventName());
        }
        else
        {
            Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

            if (!watches.isEmpty())
            {
                writeAndSend(eventIdentifier, eventStreamAction, eventWriter, watches);
            }
        }
    }

    private Collection<String> getMatchingEventNames(String eventName)
    {
        return eventName == null || eventName.isEmpty() ? eventWriters.keySet() : Collections.singleton(eventName);
    }

    private void writeAndSend(
        EventIdentifier eventIdentifier,
        String eventStreamAction,
        EventWriter eventWriter,
        Collection<Watch> watches
    )
    {
        byte[] eventData = writeEventData(eventIdentifier, eventWriter);

        if (eventData != null)
        {
            watches.forEach(
                watch -> sendEvent(
                    watch,
                    eventIdentifier,
                    eventStreamAction,
                    eventData
                ));
        }
    }

    private void writeAndSendWithEventData(
        EventIdentifier eventIdentifier,
        String eventStreamAction,
        byte[] eventData,
        Collection<Watch> watches
    )
    {
        if (eventData != null)
        {
            watches.forEach(
                watch -> sendEvent(
                    watch,
                    eventIdentifier,
                    eventStreamAction,
                    eventData
                ));
        }
    }

    private byte[] writeEventData(
        EventIdentifier eventIdentifier,
        EventWriter eventWriter
    )
    {
        byte[] eventData = null;
        try
        {
            eventData = eventWriter == null ? new byte[] {} :
                eventWriter.writeEvent(eventIdentifier.getObjectIdentifier());
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
        return eventData != null ? eventData : new byte[] {};
    }

    private void sendEvent(
        Watch watch,
        EventIdentifier eventIdentifier,
        String eventStreamAction,
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
            long eventCounter = watchStore.getAndIncrementEventCounter(watch);

            byte[] eventHeaderBytes = commonSerializer.builder(ApiConsts.API_EVENT)
                .event(watch.getPeerWatchId(), eventCounter, eventIdentifier, eventStreamAction)
                .build();

            byte[] completeData = new byte[eventHeaderBytes.length + eventData.length];
            System.arraycopy(eventHeaderBytes, 0, completeData, 0, eventHeaderBytes.length);
                System.arraycopy(eventData, 0, completeData, eventHeaderBytes.length, eventData.length);

            peer.sendMessage(completeData);
        }
    }
}
