package com.linbit.linstor.layer.bcache;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.layer.AbsCacheLayerSizeCalculator;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class BCacheLayerSizeCalculator extends AbsCacheLayerSizeCalculator<BCacheVlmData<?>>
{
    public static final long BCACHE_METADATA_SIZE_IN_KIB = 8L;
    public static final long MIN_CACHE_SIZE_IN_KIB = 512 * 1024L; // 512MB
    public static final String DFLT_CACHE_SIZE = "5%";

    @Inject
    public BCacheLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.BCACHE, ApiConsts.NAMESPC_BCACHE, BCACHE_METADATA_SIZE_IN_KIB);
        registerChildSizeCalculator(
            RscLayerSuffixes.SUFFIX_BCACHE_CACHE,
            ApiConsts.KEY_BCACHE_SIZE,
            DFLT_CACHE_SIZE,
            MIN_CACHE_SIZE_IN_KIB
        );
    }
}
