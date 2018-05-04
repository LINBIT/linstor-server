package com.linbit.linstor.event.handler;

import com.linbit.linstor.ResourceName;
import com.linbit.linstor.event.EventIdentifier;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class ResourceDefinitionEventStore
{
    private final Lock lock;
    private final Map<ResourceName, Set<EventIdentifier>> openEventStreams;

    @Inject
    public ResourceDefinitionEventStore()
    {
        lock = new ReentrantLock();
        openEventStreams = new HashMap<>();
    }

    public Lock getLock()
    {
        return lock;
    }

    /**
     * Requires lock.
     */
    public boolean contains(ResourceName resourceName)
    {
        return openEventStreams.containsKey(resourceName);
    }

    /**
     * Requires lock.
     */
    public void add(EventIdentifier eventIdentifier)
    {
        openEventStreams.compute(
            eventIdentifier.getResourceName(),
            (ignored, set) -> addToSet(set, eventIdentifier)
        );
    }

    /**
     * Requires lock.
     */
    public void remove(EventIdentifier eventIdentifier)
    {
        openEventStreams.compute(
            eventIdentifier.getResourceName(),
            (ignored, set) -> removeFromSet(set, eventIdentifier)
        );
    }

    /**
     * Thread-safe.
     */
    public Set<EventIdentifier> getEventStreamsForResource(ResourceName resourceName)
    {
        Set<EventIdentifier> ret;

        lock.lock();
        try
        {
            Set<EventIdentifier> eventIdentifiers = openEventStreams.get(resourceName);
            ret = eventIdentifiers == null ? Collections.emptySet() : new HashSet<>(eventIdentifiers);
        }
        finally
        {
            lock.unlock();
        }

        return ret;
    }

    private Set<EventIdentifier> addToSet(Set<EventIdentifier> currentSet, EventIdentifier eventIdentifier)
    {
        Set<EventIdentifier> set = currentSet == null ? new HashSet<>() : currentSet;
        set.add(eventIdentifier);
        return set;
    }

    private Set<EventIdentifier> removeFromSet(Set<EventIdentifier> set, EventIdentifier eventIdentifier)
    {
        if (set != null)
        {
            set.remove(eventIdentifier);
        }
        return set == null ? null : (set.isEmpty() ? null : set);
    }
}
