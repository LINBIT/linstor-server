package com.linbit.linstor.event;

import java.util.Collection;

/**
 * Not thread-safe; external synchronization is expected.
 */
public interface WatchStore
{
    /**
     * Add a watch.
     */
    void addWatch(Watch watch);

    /**
     * Get all watches for the identified object and all ancestor objects.
     */
    Collection<Watch> getWatchesForEvent(EventIdentifier eventIdentifier);

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
