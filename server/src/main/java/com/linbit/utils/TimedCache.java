package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TimedCache<KEY, VALUE>
{
    private final Map<KEY, PairNonNull<VALUE, Long>> mapKV = new HashMap<>();
    private final TreeMap<Long, List<KEY>> mapTime = new TreeMap<>();
    private long maxCacheTimeInMs;

    public TimedCache(long maxCacheTimeInMsRef)
    {
        maxCacheTimeInMs = maxCacheTimeInMsRef;
    }

    public long getMaxCacheTime()
    {
        return maxCacheTimeInMs;
    }

    public void setMaxCacheTime(long maxCacheTimeRef)
    {
        setMaxCacheTime(maxCacheTimeRef, false);
    }

    public void setMaxCacheTime(long maxCacheTimeRef, boolean purgeOutdatedEntriesRef)
    {
        maxCacheTimeInMs = maxCacheTimeRef;
        if (purgeOutdatedEntriesRef)
        {
            synchronized (mapKV)
            {
                purgeOld();
            }
        }
    }

    private void purgeOld()
    {
        purgeOld(System.currentTimeMillis(), maxCacheTimeInMs);
    }

    private void purgeOld(long nowRef)
    {
        purgeOld(nowRef, maxCacheTimeInMs);
    }

    private void purgeOld(long nowRef, long maxCacheTimeRef)
    {
        final long purgeTime = nowRef - maxCacheTimeRef;

        @Nullable Long floorKey = mapTime.floorKey(purgeTime);
        while (floorKey != null)
        {
            List<KEY> keysToRemove = mapTime.remove(floorKey);
            for (KEY keyToRemove : keysToRemove)
            {
                mapKV.remove(keyToRemove);
            }
            floorKey = mapTime.floorKey(purgeTime);
        }
    }

    public @Nullable VALUE get(KEY keyRef)
    {
        return get(keyRef, maxCacheTimeInMs, System.currentTimeMillis());
    }

    public @Nullable VALUE get(KEY keyRef, long nowRef)
    {
        return get(keyRef, maxCacheTimeInMs, nowRef);
    }

    public @Nullable VALUE get(KEY keyRef, long maxCacheTimeRef, long nowRef)
    {
        @Nullable VALUE ret = null;
        synchronized (mapKV)
        {
            @Nullable PairNonNull<VALUE, Long> pair = mapKV.get(keyRef);
            if (pair != null)
            {
                long createdTimestamp = pair.objB;
                if (isEntryValid(nowRef, createdTimestamp, maxCacheTimeRef))
                {
                    ret = pair.objA;
                }
                purgeOld(nowRef);
            }
            return ret;
        }
    }

    private boolean isEntryValid(long nowRef, long createdTimestampRef, long cacheTimeRef)
    {
        return createdTimestampRef + cacheTimeRef > nowRef;
    }

    public @Nullable VALUE put(KEY keyRef, VALUE valueRef)
    {
        return put(keyRef, valueRef, System.currentTimeMillis());
    }

    public @Nullable VALUE put(KEY keyRef, VALUE valueRef, long nowRef)
    {
        @Nullable PairNonNull<VALUE, Long> oldPair;
        synchronized (mapKV)
        {
            oldPair = mapKV.put(keyRef, new PairNonNull<>(valueRef, nowRef));
            if (oldPair != null)
            {
                long oldTime = oldPair.objB;
                // keyList could have been purged already via get or size call
                @Nullable List<KEY> keyList = mapTime.get(oldTime);
                if (keyList != null)
                {
                    keyList.remove(keyRef);
                    if (keyList.isEmpty())
                    {
                        mapTime.remove(oldTime);
                    }
                }
            }
            mapTime.computeIfAbsent(nowRef, ignore -> new LinkedList<>())
                .add(keyRef);
            purgeOld(nowRef);
        }
        return oldPair == null ? null : oldPair.objA;
    }

    public void clear()
    {
        synchronized (mapKV)
        {
            mapKV.clear();
            mapTime.clear();
        }
    }

    public int size()
    {
        return size(true);
    }

    public int size(boolean purgeOldRef)
    {
        return size(purgeOldRef, System.currentTimeMillis());
    }

    public int size(boolean purgeOldRef, long nowRef)
    {
        synchronized (mapKV)
        {
            if (purgeOldRef)
            {
                purgeOld(nowRef);
            }
            return mapKV.size();
        }
    }
}
