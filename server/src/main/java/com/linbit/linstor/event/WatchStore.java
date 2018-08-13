package com.linbit.linstor.event;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import reactor.core.Disposable;

/**
 * Not thread-safe; external synchronization is expected.
 */
public interface WatchStore
{
    /**
     * Add a watch.
     */
    void addWatch(Watch watch, Disposable disposable)
        throws LinStorDataAlreadyExistsException;

    /**
     * Remove all watchs for a peer.
     */
    void removeWatchesForPeer(String peerId);

    /**
     * Remove a specific watch.
     */
    void removeWatchForPeerAndId(String peerId, Integer peerWatchId);
}
