package com.linbit.linstor.event;

import com.linbit.linstor.LinStorDataAlreadyExistsException;

import java.util.Collection;

/**
 * Not thread-safe; external synchronization is expected.
 */
public interface WatchStore
{
    /**
     * Add a watch.
     */
    void addWatch(Watch watch)
        throws LinStorDataAlreadyExistsException;

    /**
     * Get all watches for the identified object and all ancestor objects.
     */
    Collection<Watch> getWatchesForEvent(EventIdentifier eventIdentifier);

    long getAndIncrementEventCounter(Watch watch);

    /**
     * Remove all watchs for a peer.
     */
    void removeWatchesForPeer(String peerId);

    /**
     * Remove a specific watch.
     */
    void removeWatchForPeerAndId(String peerId, Integer peerWatchId);

    /**
     * Remove all watchs for an object but not for descendant objects.
     */
    void removeWatchesForObject(ObjectIdentifier objectIdentifier);
}
