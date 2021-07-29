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

    // BCache
    public static final String SUFFIX_BCACHE_CACHE = ".bcache";

    private RscLayerSuffixes()
    {
    }

    public static boolean isNonMetaDataLayerSuffix(String layerSuffix)
    {
        return SUFFIX_DATA.equalsIgnoreCase(layerSuffix);
    }

    /**
     * For now, ship all rscLayerSuffixes EXCEPT SUFFIX_DRBD_META
     */
    public static boolean shouldSuffixBeShipped(String rscNameSuffixRef)
    {
        return !SUFFIX_DRBD_META.equals(rscNameSuffixRef);
    }
}
