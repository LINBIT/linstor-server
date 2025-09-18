package com.linbit.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimedCacheTest
{
    @Test
    public void simpleTest()
    {
        long now = System.currentTimeMillis();
        TimedCache<String, String> cache = new TimedCache<>(100);

        assertEquals(0, cache.size());

        cache.put("test", "a", now);
        assertEquals(1, cache.size(false));
        assertEquals("a", cache.get("test", now));
        now += 110;
        assertEquals(1, cache.size(false));
        assertEquals(null, cache.get("test", now));
        assertEquals(0, cache.size(false));
    }

    @Test
    public void concurrentEntriesTest()
    {
        long now = System.currentTimeMillis();
        TimedCache<String, String> cache = new TimedCache<>(10);

        cache.put("1", "a", now);
        cache.put("2", "b", now);
        assertEquals(2, cache.size(false));

        now += 20;
        assertEquals(2, cache.size(false));
        assertEquals(null, cache.get("1", now));
        assertEquals(0, cache.size(false));
        assertEquals(null, cache.get("2", now));
        assertEquals(0, cache.size(false));
    }

    @Test
    public void concurrentEntriesWithUpdateTest()
    {
        TimedCache<String, String> cache = new TimedCache<>(50);

        long now = System.currentTimeMillis();
        cache.put("1", "a", now);
        cache.put("2", "b", now);
        assertEquals(2, cache.size());

        now += 30;
        assertEquals(2, cache.size(false));
        cache.put("1", "a", now);
        assertEquals(2, cache.size(false));

        now += 30;
        assertEquals(2, cache.size(false));
        assertEquals("a", cache.get("1", now));
        assertEquals(1, cache.size(false));
        assertEquals(null, cache.get("2", now));
        assertEquals(1, cache.size(false));

        now += 30;
        assertEquals(1, cache.size(false));
        assertEquals(null, cache.get("1", now));
        assertEquals(0, cache.size(false));
    }

    @Test
    public void someoneScrewingWithTheSystemTimeTest()
    {
        TimedCache<String, String> cache = new TimedCache<>(50);

        long now = System.currentTimeMillis();
        cache.put("1", "a", now);
        assertEquals(1, cache.size());

        now -= 20;
        assertEquals(1, cache.size(false));
        assertEquals("a", cache.get("1", now));
        assertEquals(1, cache.size(false));

        now += 80;
        assertEquals(1, cache.size(false));
        assertEquals(null, cache.get("1", now));
        assertEquals(0, cache.size(false));
    }

    @Test
    public void unreasonablyHighMaxCacheTimeTest()
    {
        long originalNow = System.currentTimeMillis();
        long now = originalNow;
        TimedCache<String, String> cache = new TimedCache<>(now + 1_000_000);

        cache.put("1", "a", now);
        assertEquals(1, cache.size());

        now += 20;
        assertEquals(1, cache.size(false));
        assertEquals("a", cache.get("1", now));
        assertEquals(1, cache.size(false));

        now += 1_000_000;
        assertEquals(1, cache.size(false));
        assertEquals("a", cache.get("1", now));
        assertEquals(1, cache.size(false));

        now += originalNow - 100;
        assertEquals(1, cache.size(false));
        assertEquals("a", cache.get("1", now));
        assertEquals(1, cache.size(false));

        now += 100;
        assertEquals(1, cache.size(false));
        assertEquals(null, cache.get("1", now));
        assertEquals(0, cache.size(false));
    }
}
