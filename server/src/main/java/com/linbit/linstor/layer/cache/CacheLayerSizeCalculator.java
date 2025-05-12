package com.linbit.linstor.layer.cache;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsCacheLayerSizeCalculator;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CacheLayerSizeCalculator extends AbsCacheLayerSizeCalculator<CacheVlmData<?>>
{
    public static final String DFLT_CACHE_SIZE = "5%";
    public static final long DFLT_BLOCK_SIZE_IN_BYTES = 4096; // 4KiB
    /**
     * Documentation states that the formula for the meta device size should be 4MB + 16B * $nrOfBlocks
     * This constant is only the 4M part in bytes.
     */
    private static final long META_BASE_SIZE_IN_BYTES = 4 << 20;
    private static final long META_BYTES_PER_BLOCK = 16;

    @Inject
    public CacheLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.CACHE, ApiConsts.NAMESPC_CACHE, 0);
        registerChildSizeCalculator(
            RscLayerSuffixes.SUFFIX_CACHE_CACHE,
            ApiConsts.KEY_CACHE_CACHE_SIZE,
            DFLT_CACHE_SIZE
        );
        registerChildSizeCalculator(RscLayerSuffixes.SUFFIX_CACHE_META, this::calcMetaDeviceSize);
    }


    private long calcMetaDeviceSize(CacheVlmData<?> vlmDataRef, VlmProviderObject<?> cacheChildVlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        long cacheSizeInKib = vlmDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_CACHE).getUsableSize();
        long cacheSizeInB = SizeConv.convert(cacheSizeInKib, SizeUnit.UNIT_KiB, SizeUnit.UNIT_B);
        @Nullable String blockSizeStr = getPrioProps(vlmDataRef.getVolume()).getProp(
            ApiConsts.KEY_CACHE_BLOCK_SIZE,
            ApiConsts.NAMESPC_CACHE
        );
        long blockSizeInB = blockSizeStr == null ? DFLT_BLOCK_SIZE_IN_BYTES : Long.parseLong(blockSizeStr);
        long nrOfBlocks = cacheSizeInB / blockSizeInB;
        if (cacheSizeInB % blockSizeInB != 0)
        {
            nrOfBlocks++;
        }
        long dfltMetaSizeInB = META_BASE_SIZE_IN_BYTES + nrOfBlocks * META_BYTES_PER_BLOCK;
        long dfltMetaSizeInKib = SizeConv.convert(dfltMetaSizeInB, SizeUnit.UNIT_B, SizeUnit.UNIT_KiB);

        return getCacheSize(
            cacheChildVlmDataRef,
            vlmDataRef.getUsableSize(),
            ApiConsts.KEY_CACHE_META_SIZE,
            Long.toString(dfltMetaSizeInKib),
            1
        );
    }
}
