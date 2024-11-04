package com.linbit.linstor.event;

import com.linbit.linstor.LinStorDataAlreadyExistsException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import reactor.core.Disposable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class WatchStoreImpl implements WatchStore
{
    private final Map<String, Map<Integer, Tuple2<Watch, Disposable>>> watchesByPeer = new HashMap<>();

    @Inject
    public WatchStoreImpl()
    {
    }

    @Override
    public void addWatch(Watch watch, Disposable disposable)
        throws LinStorDataAlreadyExistsException
    {
        Map<Integer, Tuple2<Watch, Disposable>> peerWatches = watchesByPeer.get(watch.getPeerId());
        if (peerWatches != null)
        {
            if (peerWatches.containsKey(watch.getPeerWatchId()))
            {
                throw new LinStorDataAlreadyExistsException("Watch with this ID already exists");
            }
        }

        String peerId = watch.getPeerId();
        Integer peerWatchId = watch.getPeerWatchId();
        watchesByPeer.computeIfAbsent(peerId, ignored -> new HashMap<>())
            .put(peerWatchId, Tuples.of(watch, disposable));
    }

    @Override
    public void removeWatchesForPeer(String peerId)
    {
        Map<Integer, Tuple2<Watch, Disposable>> watchesForPeer = watchesByPeer.remove(peerId);
        if (watchesForPeer != null)
        {
            removeWatches(watchesForPeer.values());
        }
    }

    @Override
    public void removeWatchForPeerAndId(String peerId, Integer peerWatchId)
    {
        Map<Integer, Tuple2<Watch, Disposable>> watchesForPeer = watchesByPeer.get(peerId);
        if (watchesForPeer != null)
        {
            Tuple2<Watch, Disposable> watch = watchesForPeer.remove(peerWatchId);
            if (watch != null)
            {
                removeWatches(Collections.singleton(watch));
            }
        }
    }

    private void removeWatches(Collection<Tuple2<Watch, Disposable>> watches)
    {
        for (Tuple2<Watch, Disposable> watchAndDisposable : watches)
        {
            watchAndDisposable.getT2().dispose();
        }
    }
}
