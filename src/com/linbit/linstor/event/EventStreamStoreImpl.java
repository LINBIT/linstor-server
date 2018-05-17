package com.linbit.linstor.event;

import com.linbit.linstor.LinStorDataAlreadyExistsException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventStreamStoreImpl implements EventStreamStore
{
    private final Set<EventIdentifier> allStreams;
    private final Map<EventIdentifier, Set<EventIdentifier>> childrenMap;

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
            throw new LinStorDataAlreadyExistsException("Event stream " + eventIdentifier + " already exists");
        }

        allStreams.add(eventIdentifier);
        insertRecursively(eventIdentifier);
    }

    @Override
    public boolean addEventStreamIfNew(EventIdentifier eventIdentifier)
    {
        boolean isNew = allStreams.add(eventIdentifier);
        if (isNew)
        {
            insertRecursively(eventIdentifier);
        }
        return isNew;
    }

    @Override
    public boolean removeEventStream(EventIdentifier eventIdentifier)
    {
        boolean wasPresent = allStreams.remove(eventIdentifier);
        removeRecursively(eventIdentifier);
        return wasPresent;
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
            // Node: no
            if (eventIdentifier.getResourceName() == null)
            {
                // Node: no, resource: no => Root
                parents = Collections.emptySet();
            }
            else
            {
                // Node: no, resource: yes
                if (eventIdentifier.getVolumeNumber() == null)
                {
                    // Node: no, resource: yes, volume: no
                    if (eventIdentifier.getSnapshotName() == null)
                    {
                        // Node: no, resource: yes, volume: no, snapshot: no => Resource definition
                        parents = Collections.singleton(EventIdentifier.global(eventIdentifier.getEventName()));
                    }
                    else
                    {
                        // Node: no, resource: yes, volume: no, snapshot: yes => Snapshot definition
                        parents = Collections.singleton(EventIdentifier.resourceDefinition(
                            eventIdentifier.getEventName(), eventIdentifier.getResourceName()
                        ));
                    }
                }
                else
                {
                    // Node: no, resource: yes, volume: yes => Volume definition
                    parents = Collections.singleton(EventIdentifier.resourceDefinition(
                        eventIdentifier.getEventName(), eventIdentifier.getResourceName()
                    ));
                }
            }
        }
        else
        {
            // Node: yes
            if (eventIdentifier.getResourceName() == null)
            {
                // Node: yes, resource: no => Node
                parents = Collections.singleton(EventIdentifier.global(eventIdentifier.getEventName()));
            }
            else
            {
                // Node: yes, resource: yes
                if (eventIdentifier.getVolumeNumber() == null)
                {
                    // Node: yes, resource: yes, volume: no
                    if (eventIdentifier.getSnapshotName() == null)
                    {
                        // Node: yes, resource: yes, volume: no, snapshot: no => Resource
                        parents = Stream.of(
                            EventIdentifier.resourceDefinition(
                                eventIdentifier.getEventName(), eventIdentifier.getResourceName()),
                            EventIdentifier.node(
                                eventIdentifier.getEventName(), eventIdentifier.getNodeName())
                        ).collect(Collectors.toSet());
                    }
                    else
                    {
                        // Node: yes, resource: yes, volume: no, snapshot: yes => Snapshot
                        parents = Stream.of(
                            EventIdentifier.resource(
                                eventIdentifier.getEventName(),
                                eventIdentifier.getNodeName(),
                                eventIdentifier.getResourceName()
                            ),
                            EventIdentifier.snapshotDefinition(
                                eventIdentifier.getEventName(),
                                eventIdentifier.getResourceName(),
                                eventIdentifier.getSnapshotName()
                            )
                        ).collect(Collectors.toSet());
                    }
                }
                else
                {
                    // Node: yes, resource: yes, volume: yes => Volume
                    parents = Stream.of(
                        EventIdentifier.volumeDefinition(
                            eventIdentifier.getEventName(),
                            eventIdentifier.getResourceName(),
                            eventIdentifier.getVolumeNumber()
                        ),
                        EventIdentifier.resource(
                            eventIdentifier.getEventName(),
                            eventIdentifier.getNodeName(),
                            eventIdentifier.getResourceName()
                        )
                    ).collect(Collectors.toSet());
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
