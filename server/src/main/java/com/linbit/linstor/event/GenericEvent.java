package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.netcom.PeerNotConnectedException;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class GenericEvent<T> implements LinstorTriggerableEvent<T>
{
    private final Scheduler scheduler;

    // Lock protecting internal data structures; must not be held when emitting
    private final Lock lock = new ReentrantLock();

    // Sinks where new values are pushed to
    private final Map<ObjectIdentifier, Sinks.Many<T>> sinks = new HashMap<>();

    // Streams which can be subscribed to
    private final Map<ObjectIdentifier, Flux<T>> streams = new HashMap<>();

    // What streams are present in hierarchical form so that we can find the relevant streams when a watch is created
    private final EventStreamStore eventStreamStore = new EventStreamStoreImpl();

    // Watches that are open and waiting for streams to appear
    private final Map<ObjectIdentifier, Set<FluxSink<Tuple2<ObjectIdentifier, Flux<T>>>>> waiters = new HashMap<>();

    @Inject
    public GenericEvent(Scheduler schedulerRef)
    {
        scheduler = schedulerRef;
    }

    @Override
    public Flux<ObjectSignal<T>> watchForStreams(ObjectIdentifier ancestor)
    {
        return Flux
            .<Tuple2<ObjectIdentifier, Flux<T>>>create(fluxSink ->
                {
                    fluxSink.onDispose(() ->
                        {
                            lock.lock();
                            try
                            {
                                Set<FluxSink<Tuple2<ObjectIdentifier, Flux<T>>>> waiterSet = waiters.get(ancestor);
                                if (waiterSet != null)
                                {
                                    waiterSet.remove(fluxSink);
                                    if (waiterSet.isEmpty())
                                    {
                                        waiters.remove(ancestor);
                                    }
                                }
                            }
                            finally
                            {
                                lock.unlock();
                            }
                        }
                    );

                    lock.lock();
                    try
                    {
                        Collection<EventIdentifier> descendantEventStreams = eventStreamStore.getDescendantEventStreams(
                            new EventIdentifier(null, ancestor));

                        for (EventIdentifier descendantEventStream : descendantEventStreams)
                        {
                            ObjectIdentifier objectIdentifier = descendantEventStream.getObjectIdentifier();
                            fluxSink.next(Tuples.of(objectIdentifier, streams.get(objectIdentifier)));
                        }

                        waiters.computeIfAbsent(ancestor, ignored -> new HashSet<>()).add(fluxSink);
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }
            )
            .flatMap(
                objectStream -> objectStream.getT2()
                    .materialize()
                    .map(signal -> new ObjectSignal<>(objectStream.getT1(), signal)),
                // Allow the watch to subscribe to more than reactor.util.concurrent.Queues.SMALL_BUFFER_SIZE streams
                // concurrently
                Integer.MAX_VALUE
            );
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void triggerEvent(ObjectIdentifier objectIdentifier, T value)
    {
        Flux<T> stream = null;
        Sinks.Many<T> sink;
        Set<FluxSink<Tuple2<ObjectIdentifier, Flux<T>>>> waiterSet = null;

        lock.lock();
        try
        {
            sink = sinks.get(objectIdentifier);
            if (sink == null)
            {
                sink = Sinks.many().unicast().onBackpressureBuffer();
                ConnectableFlux<T> publisher = sink.asFlux().replay(1);
                publisher.connect();

                // Publish events signals on the main scheduler to detach the execution from this thread,
                // so that we don't react to events in the thread-local context where the event is triggered.
                stream = publisher.publishOn(scheduler);
                streams.put(objectIdentifier, stream);
                try
                {
                    eventStreamStore.addEventStream(new EventIdentifier(null, objectIdentifier));
                }
                catch (LinStorDataAlreadyExistsException exc)
                {
                    throw new ImplementationError(exc);
                }

                sinks.put(objectIdentifier, sink);

                List<ObjectIdentifier> matchingWaitObjects = matchingObjects(objectIdentifier);

                waiterSet = new HashSet<>();
                for (ObjectIdentifier waitObject : matchingWaitObjects)
                {
                    Set<FluxSink<Tuple2<ObjectIdentifier, Flux<T>>>> waitersForObject = waiters.get(waitObject);
                    if (waitersForObject != null)
                    {
                        waiterSet.addAll(waitersForObject);
                    }
                }
            }
        }
        finally
        {
            lock.unlock();
        }

        if (waiterSet != null)
        {
            for (FluxSink<Tuple2<ObjectIdentifier, Flux<T>>> waiter : waiterSet)
            {
                waiter.next(Tuples.of(objectIdentifier, stream));
            }
        }

        while (sink.tryEmitNext(value) == Sinks.EmitResult.FAIL_NON_SERIALIZED)
        {
            LockSupport.parkNanos(10);
        }
    }

    @Override
    public void closeStream(ObjectIdentifier objectIdentifier)
    {
        Sinks.Many<T> sink = removeStream(objectIdentifier);

        if (sink != null)
        {
            sink.tryEmitComplete();
        }
    }

    @Override
    public void closeStreamNoConnection(ObjectIdentifier objectIdentifier)
    {
        Sinks.Many<T> sink = removeStream(objectIdentifier);

        if (sink != null)
        {
            sink.tryEmitError(new PeerNotConnectedException());
        }
    }

    /**
     * Get all ancestor objects, including the object itself.
     */
    private List<ObjectIdentifier> matchingObjects(ObjectIdentifier objectIdentifier)
    {
        List<NodeName> nodeNames = objectIdentifier.getNodeName() == null ?
            Collections.singletonList(null) : Arrays.asList(null, objectIdentifier.getNodeName());

        List<ResourceName> resourceNames = objectIdentifier.getResourceName() == null ?
            Collections.singletonList(null) : Arrays.asList(null, objectIdentifier.getResourceName());

        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        for (NodeName nodeName : nodeNames)
        {
            for (ResourceName resourceName : resourceNames)
            {
                List<VolumeNumber> volumeNumbers =
                    (resourceName == null || objectIdentifier.getVolumeNumber() == null) ?
                        Collections.singletonList(null) : Arrays.asList(null, objectIdentifier.getVolumeNumber());

                for (VolumeNumber volumeNumber : volumeNumbers)
                {
                    List<SnapshotName> snapshotNames =
                        (resourceName == null || volumeNumber != null || objectIdentifier.getSnapshotName() == null) ?
                            Collections.singletonList(null) : Arrays.asList(null, objectIdentifier.getSnapshotName());

                    for (SnapshotName snapshotName : snapshotNames)
                    {
                        objectIdentifiers.add(
                            new ObjectIdentifier(nodeName, resourceName, volumeNumber, snapshotName, null));
                    }
                }
            }
        }
        return objectIdentifiers;
    }

    private @Nullable Sinks.Many<T> removeStream(ObjectIdentifier objectIdentifier)
    {
        Sinks.Many<T> sink;

        lock.lock();
        try
        {
            sink = sinks.remove(objectIdentifier);
            if (sink != null)
            {
                eventStreamStore.removeEventStream(new EventIdentifier(null, objectIdentifier));
                streams.remove(objectIdentifier);
            }
        }
        finally
        {
            lock.unlock();
        }

        return sink;
    }
}
