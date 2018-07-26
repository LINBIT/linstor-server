package com.linbit.linstor.event;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.VolumeNumber;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Singleton
public class WatchStoreImpl implements WatchStore
{
    private final Map<ObjectIdentifier, Set<Watch>> watchesByObject = new HashMap<>();
    private final Map<String, Map<Integer, Watch>> watchesByPeer = new HashMap<>();
    private final Map<Watch, Long> eventCounterNext = new HashMap<>();

    @Inject
    public WatchStoreImpl()
    {
    }

    @Override
    public void addWatch(Watch watch)
        throws LinStorDataAlreadyExistsException
    {
        Map<Integer, Watch> peerWatches = watchesByPeer.get(watch.getPeerId());
        if (peerWatches != null)
        {
            if (peerWatches.containsKey(watch.getPeerWatchId()))
            {
                throw new LinStorDataAlreadyExistsException("Watch with this ID already exists");
            }
        }

        ObjectIdentifier objectIdentifier = watch.getEventIdentifier().getObjectIdentifier();

        putMultiMap(watchesByObject, objectIdentifier, watch);

        String peerId = watch.getPeerId();
        Integer peerWatchId = watch.getPeerWatchId();
        if (peerId != null && peerWatchId != null)
        {
            getInsertingDefault(watchesByPeer, peerId, HashMap::new).put(peerWatchId, watch);

            eventCounterNext.put(watch, 1L);
        }
    }

    @Override
    public Collection<Watch> getWatchesForEvent(EventIdentifier eventIdentifier)
    {
        return matchingObjects(eventIdentifier).stream()
            .map(watchesByObject::get)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    @Override
    public long getAndIncrementEventCounter(Watch watch)
    {
        long counter = eventCounterNext.get(watch);
        eventCounterNext.put(watch, counter + 1);
        return counter;
    }

    @Override
    public void removeWatchesForPeer(String peerId)
    {
        removeWatches(
            Optional.ofNullable(watchesByPeer.get(peerId))
                .map(Map::values)
                // Copy the value set to avoid removing elements from it while iterating over it
                .<Set<Watch>>map(HashSet::new)
                .orElse(Collections.emptySet())
        );
    }

    @Override
    public void removeWatchForPeerAndId(String peerId, Integer peerWatchId)
    {
        removeWatches(
            Optional.ofNullable(watchesByPeer.get(peerId))
                .flatMap(peerWatchIdMap -> Optional.ofNullable(peerWatchIdMap.get(peerWatchId)))
                .map(Collections::singleton)
                .orElse(Collections.emptySet())
        );
    }

    @Override
    public void removeWatchesForObject(ObjectIdentifier objectIdentifier)
    {
        // Copy the value set to avoid removing elements from it while iterating over it
        removeWatches(new HashSet<>(watchesByObject.get(objectIdentifier)));
    }

    private <K, V> V getInsertingDefault(Map<K, V> map, K key, Supplier<V> defaultSupplier)
    {
        map.computeIfAbsent(key, ignored -> defaultSupplier.get());
        return map.get(key);
    }

    private <K, V> void putMultiMap(Map<K, Set<V>> multiMap, K key, V value)
    {
        getInsertingDefault(multiMap, key, HashSet::new).add(value);
    }

    private <V> Set<V> removeCollapsingEmpty(Set<V> set, V value)
    {
        set.remove(value);
        return set.isEmpty() ? null : set;
    }

    private <K, V> void removeMultiMap(Map<K, Set<V>> multiMap, K key, V value)
    {
        multiMap.computeIfPresent(key, (ignored, values) -> removeCollapsingEmpty(values, value));
    }

    private <K, V> Map<K, V> removeMapCollapsingEmpty(Map<K, V> map, K key)
    {
        map.remove(key);
        return map.isEmpty() ? null : map;
    }

    /**
     * Get all ancestor objects, including the object itself.
     */
    private List<ObjectIdentifier> matchingObjects(EventIdentifier eventIdentifier)
    {
        List<NodeName> nodeNames = eventIdentifier.getNodeName() == null ?
            Collections.singletonList(null) : Arrays.asList(null, eventIdentifier.getNodeName());

        List<ResourceName> resourceNames = eventIdentifier.getResourceName() == null ?
            Collections.singletonList(null) : Arrays.asList(null, eventIdentifier.getResourceName());

        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        for (NodeName nodeName : nodeNames)
        {
            for (ResourceName resourceName : resourceNames)
            {
                List<VolumeNumber> volumeNumbers =
                    (resourceName == null || eventIdentifier.getVolumeNumber() == null) ?
                    Collections.singletonList(null) : Arrays.asList(null, eventIdentifier.getVolumeNumber());

                for (VolumeNumber volumeNumber : volumeNumbers)
                {
                    List<SnapshotName> snapshotNames =
                        (resourceName == null || volumeNumber != null || eventIdentifier.getSnapshotName() == null) ?
                            Collections.singletonList(null) : Arrays.asList(null, eventIdentifier.getSnapshotName());

                    for (SnapshotName snapshotName : snapshotNames)
                    {
                        objectIdentifiers.add(new ObjectIdentifier(nodeName, resourceName, volumeNumber, snapshotName));
                    }
                }
            }
        }
        return objectIdentifiers;
    }

    private void removeWatches(Collection<Watch> watches)
    {
        watches.forEach(this::removeWatch);
    }

    private void removeWatch(Watch watch)
    {
        removeMultiMap(watchesByObject,
            watch.getEventIdentifier().getObjectIdentifier(), watch);

        String peerId = watch.getPeerId();
        Integer peerWatchId = watch.getPeerWatchId();
        if (peerId != null && peerWatchId != null)
        {
            watchesByPeer.computeIfPresent(peerId,
                (ignored, peerWatchIdMap) ->
                    removeMapCollapsingEmpty(peerWatchIdMap, peerWatchId)
            );

            eventCounterNext.remove(watch);
        }
    }
}
