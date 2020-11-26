package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;

@Singleton
public class EventBroker
{
    private final ErrorReporter errorReporter;
    private final CommonSerializer commonSerializer;
    private final WatchStore watchStore;
    private final Map<String, EventSerializer> eventSerializers;
    private final Map<String, EventSerializerDescriptor> eventSerializerDescriptors;
    private final ReentrantLock watchLock;

    @Inject
    public EventBroker(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        WatchStore watchStoreRef,
        Map<String, EventSerializer> eventSerializersRef,
        Map<String, EventSerializerDescriptor> eventSerializerDescriptorsRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        watchStore = watchStoreRef;
        eventSerializers = eventSerializersRef;
        eventSerializerDescriptors = eventSerializerDescriptorsRef;

        watchLock = new ReentrantLock();
    }

    /**
     * Add a watch and send initial state for all relevant events.
     */
    public void createWatch(Peer peer, Watch watch)
    {
        errorReporter.logTrace("Create watch for: %s", watch.getEventIdentifier());
        watchLock.lock();
        try
        {
            Collection<String> eventNames = getMatchingEventNames(watch.getEventIdentifier().getEventName());

            List<Flux<byte[]>> watchStreams = new ArrayList<>();

            for (String eventName : eventNames)
            {
                EventSerializer eventSerializer = eventSerializers.get(eventName);
                EventSerializerDescriptor eventSerializerDescriptor = eventSerializerDescriptors.get(eventName);

                if (eventSerializer == null || eventSerializerDescriptor == null)
                {
                    throw new UnknownEventException(eventName);
                }

                watchStreams.add(
                    createWatchForEvent(watch, eventSerializer.get(), eventSerializerDescriptor.getEventName()));
            }

            Flux<byte[]> mergedStreams = Flux.merge(watchStreams);

            Disposable disposable = mergedStreams
                .subscribe(
                    peer::sendMessage,
                    exception -> errorReporter.reportError(exception, null, null, "Uncaught exception sending event")
                );

            watchStore.addWatch(watch, disposable);
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            errorReporter.logError(
                "Watch already exists for peer " + watch.getPeerId() + ", id " + watch.getPeerWatchId());
        }
        finally
        {
            watchLock.unlock();
        }
        errorReporter.logTrace("Create watch done");
    }

    public void deleteWatch(String peerId, int peerWatchId)
    {
        errorReporter.logTrace("Event 'delete watch' start");
        watchLock.lock();
        try
        {
            watchStore.removeWatchForPeerAndId(peerId, peerWatchId);
        }
        finally
        {
            watchLock.unlock();
        }
        errorReporter.logTrace("Event 'delete watch' end");
    }

    public void connectionClosed(Peer peer)
    {
        errorReporter.logTrace("Event 'connection closed' start: %s", peer);
        watchLock.lock();
        try
        {
            watchStore.removeWatchesForPeer(peer.getId());
        }
        finally
        {
            watchLock.unlock();
        }
        errorReporter.logTrace("Event 'connection closed' end: %s", peer);
    }

    private <T> Flux<byte[]> createWatchForEvent(
        Watch watch,
        EventSerializer.Serializer<T> eventSerializer,
        String eventName
    )
    {
        return eventSerializer.getEvent()
            .watchForStreams(watch.getEventIdentifier().getObjectIdentifier())
            .map(objectSignal -> serializeSignal(watch.getPeerWatchId(), eventSerializer, eventName, objectSignal));
    }

    private <T> byte[] serializeSignal(
        Integer peerWatchId,
        EventSerializer.Serializer<T> eventSerializer,
        String eventName,
        ObjectSignal<T> objectSignal
    )
    {
        EventIdentifier eventIdentifier = new EventIdentifier(eventName, objectSignal.getObjectIdentifier());
        Signal<T> signal = objectSignal.getSignal();

        CommonSerializer.CommonSerializerBuilder builder = commonSerializer.onewayBuilder(ApiConsts.API_EVENT);
        if (signal.isOnNext())
        {
            builder
                .event(peerWatchId, eventIdentifier, InternalApiConsts.EVENT_STREAM_VALUE)
                .bytes(eventSerializer.writeEventValue(signal.get()));
        }
        else
        if (signal.isOnComplete())
        {
            builder.event(peerWatchId, eventIdentifier, InternalApiConsts.EVENT_STREAM_CLOSE_REMOVED);
        }
        else
        if (signal.isOnError() && signal.getThrowable() instanceof PeerNotConnectedException)
        {
            builder.event(peerWatchId, eventIdentifier, InternalApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION);
        }
        else
        {
            throw new ImplementationError("Unexpected event signal " + signal);
        }
        return builder.build();
    }

    private Collection<String> getMatchingEventNames(String eventName)
    {
        return eventName == null || eventName.isEmpty() ? eventSerializers.keySet() : Collections.singleton(eventName);
    }
}
