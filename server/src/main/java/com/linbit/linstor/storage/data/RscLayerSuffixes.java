package com.linbit.linstor.storage.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static final List<String> SUFFIXES_TO_SHIP;

    static
    {
        SUFFIXES_TO_SHIP = Collections.unmodifiableList(Arrays.asList(SUFFIX_DATA, SUFFIX_DRBD_META));
    }

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
        return SUFFIXES_TO_SHIP.contains(rscNameSuffixRef);
    }
}
