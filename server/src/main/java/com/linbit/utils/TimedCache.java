package com.linbit.utils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TimedCache<KEY, VALUE>
{
    private final Map<KEY, Pair<VALUE, Long>> map = new HashMap<>();
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
            synchronized (map)
            {
                final long now = System.currentTimeMillis();
                List<KEY> keysToRemove = new ArrayList<>();
                for (Entry<KEY, Pair<VALUE, Long>> entry : map.entrySet())
                {
                    if (!isEntryValid(now, entry.getValue().objB, maxCacheTimeRef))
                    {
                        keysToRemove.add(entry.getKey());
                    }
                }
                for (KEY keyToRemove : keysToRemove)
                {
                    map.remove(keyToRemove);
                }
            }
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
        synchronized (map)
        {
            @Nullable Pair<VALUE, Long> pair = map.get(keyRef);
            if (pair != null)
            {
                long createdTimestamp = pair.objB;
                if (isEntryValid(nowRef, createdTimestamp, maxCacheTimeRef))
                {
                    ret = pair.objA;
                }
                // technically we could remove the key from the map, but usually if we have a cache-miss a corresponding
                // lvs/vgs is called shortly after which will then again populate this map with the given key
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
        @Nullable Pair<VALUE, Long> oldPair;
        synchronized (map)
        {
            oldPair = map.put(keyRef, new Pair<>(valueRef, nowRef));
        }
        return oldPair == null ? null : oldPair.objA;
    }

    public void clear()
    {
        synchronized (map)
        {
            map.clear();
        }
    }
}
