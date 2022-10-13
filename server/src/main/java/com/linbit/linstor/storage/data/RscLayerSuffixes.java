package com.linbit.linstor.storage.data;

import com.linbit.ImplementationError;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

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
        SUFFIXES_TO_SHIP = Collections.unmodifiableList(
            Arrays.asList(
                SUFFIX_DATA
                /*
                 * Metadata does not need to be shipped since it will be recreated anyways
                 */
            )
        );
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

    public static DeviceLayerKind getLayerKindFromLastSuffix(String rscNameSuffixRef)
    {
        int lastDot = rscNameSuffixRef.lastIndexOf(".");
        return getLayerKindBySuffix(lastDot <= 0 ? rscNameSuffixRef : rscNameSuffixRef.substring(lastDot));
    }

    public static DeviceLayerKind getLayerKindBySuffix(String rscNameSuffixRef)
    {
        final DeviceLayerKind selectedKind;
        switch (rscNameSuffixRef)
        {
            case SUFFIX_DATA:
                selectedKind = DeviceLayerKind.STORAGE;
                break;
            case SUFFIX_DRBD_META:
                selectedKind = DeviceLayerKind.DRBD;
                break;
            case SUFFIX_WRITECACHE_CACHE:
                selectedKind = DeviceLayerKind.WRITECACHE;
                break;
            case SUFFIX_BCACHE_CACHE:
                selectedKind = DeviceLayerKind.BCACHE;
                break;
            case SUFFIX_CACHE_CACHE:
                // fall-through
            case SUFFIX_CACHE_META:
                selectedKind = DeviceLayerKind.CACHE;
                break;
            default:
                throw new ImplementationError("Unknown RscLayerSuffix given: " + rscNameSuffixRef);
        }
        return selectedKind;
    }
}
