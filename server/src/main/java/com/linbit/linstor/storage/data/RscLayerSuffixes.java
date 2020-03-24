package com.linbit.linstor.storage.data;

public final class RscLayerSuffixes
{
    public static final String SUFFIX_DATA = "";

    // DRBD
    public static final String SUFFIX_DRBD_META = ".meta";

    // LUKS

    // NVMe

    // Writecache
    public static final String SUFFIX_WRITECACHE_CACHE = ".cache";

    // Cache
    public static final String SUFFIX_CACHE_CACHE = ".dmcache_cache"; // just like moon moon
    public static final String SUFFIX_CACHE_META = ".dmcache_meta";

    private RscLayerSuffixes()
    {
    }
}
