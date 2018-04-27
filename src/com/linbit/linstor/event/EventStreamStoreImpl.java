package com.linbit.linstor.event;

import com.linbit.linstor.LinStorDataAlreadyExistsException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class EventStreamStoreImpl implements EventStreamStore
{
    private final Set<EventIdentifier> allStreams;
    private final Map<EventIdentifier, Set<EventIdentifier>> childrenMap;

    @Inject
    public EventStreamStoreImpl()
    {
        allStreams = new HashSet<>();
        childrenMap = new HashMap<>();
    }

    @Override
    public void addEventStream(EventIdentifier eventIdentifier)
        throws LinStorDataAlreadyExistsException
    {
        if (allStreams.contains(eventIdentifier))
        {
            throw new LinStorDataAlreadyExistsException("Event stream already exists");
        }

        allStreams.add(eventIdentifier);
        insertRecursively(eventIdentifier);
    }

    @Override
    public void removeEventStream(EventIdentifier eventIdentifier)
    {
        allStreams.remove(eventIdentifier);
        removeRecursively(eventIdentifier);
    }

    @Override
    public Collection<EventIdentifier> getDescendantEventStreams(EventIdentifier eventIdentifier)
    {
        Set<EventIdentifier> collector = new HashSet<>();
        getRecursively(eventIdentifier, collector, new HashSet<>());
        return collector;
    }

    private void insertRecursively(EventIdentifier eventIdentifier)
    {
        Set<EventIdentifier> parents = getParents(eventIdentifier);

        for (EventIdentifier parent : parents)
        {
            childrenMap.computeIfAbsent(parent, ignored -> new HashSet<>());

            Set<EventIdentifier> childrenOfParent = childrenMap.get(parent);
            if (!childrenOfParent.contains(eventIdentifier))
            {
                childrenOfParent.add(eventIdentifier);
                insertRecursively(parent);
            }
        }
    }

    private void removeRecursively(EventIdentifier eventIdentifier)
    {
        Set<EventIdentifier> parents = getParents(eventIdentifier);

        for (EventIdentifier parent : parents)
        {
            Set<EventIdentifier> childrenOfParent = childrenMap.get(parent);
            if (childrenOfParent != null && childrenOfParent.contains(eventIdentifier))
            {
                childrenOfParent.remove(eventIdentifier);
                if (childrenOfParent.isEmpty())
                {
                    childrenMap.remove(parent);
                }
                removeRecursively(parent);
            }
        }
    }

    private Set<EventIdentifier> getParents(EventIdentifier eventIdentifier)
    {
        Set<EventIdentifier> parents;

        if (eventIdentifier.getNodeName() == null)
        {
            // No node specified
            if (eventIdentifier.getResourceName() == null)
            {
                // No resource specified => Root
                parents = Collections.emptySet();
            }
            else
            {
                // Resource specified
                if (eventIdentifier.getVolumeNumber() == null)
                {
                    // No volume specified => Resource definition
                    parents = Collections.singleton(new EventIdentifier(
                        eventIdentifier.getEventName(),
                        null,
                        null,
                        null
                    ));
                }
                else
                {
                    // Volume specified => Volume definition
                    parents = Collections.singleton(new EventIdentifier(
                        eventIdentifier.getEventName(),
                        null,
                        eventIdentifier.getResourceName(),
                        null
                    ));
                }
            }
        }
        else
        {
            // Node specified
            if (eventIdentifier.getResourceName() == null)
            {
                // No resource specified => Node
                parents = Collections.singleton(new EventIdentifier(
                    eventIdentifier.getEventName(),
                    null,
                    null,
                    null
                ));
            }
            else
            {
                // Resource specified
                if (eventIdentifier.getVolumeNumber() == null)
                {
                    // No volume specified => Resource
                    parents = Stream.of(new EventIdentifier(
                        eventIdentifier.getEventName(),
                        null,
                        eventIdentifier.getResourceName(),
                        null
                    ), new EventIdentifier(
                        eventIdentifier.getEventName(),
                        eventIdentifier.getNodeName(),
                        null,
                        null
                    )).collect(Collectors.toSet());
                }
                else
                {
                    // Volume specified => Volume
                    parents = Stream.of(new EventIdentifier(
                        eventIdentifier.getEventName(),
                        null,
                        eventIdentifier.getResourceName(),
                        eventIdentifier.getVolumeNumber()
                    ), new EventIdentifier(
                        eventIdentifier.getEventName(),
                        eventIdentifier.getNodeName(),
                        eventIdentifier.getResourceName(),
                        null
                    )).collect(Collectors.toSet());
                }
            }
        }

        return parents;
    }

    private void getRecursively(
        EventIdentifier eventIdentifier,
        Set<EventIdentifier> collector,
        Set<EventIdentifier> visited
    )
    {
        if (!visited.contains(eventIdentifier))
        {
            visited.add(eventIdentifier);
            if (allStreams.contains(eventIdentifier))
            {
                collector.add(eventIdentifier);
            }

            Set<EventIdentifier> children = childrenMap.get(eventIdentifier);
            if (children != null)
            {
                for (EventIdentifier child : children)
                {
                    getRecursively(child, collector, visited);
                }
            }
        }
    }
}
