package com.linbit.linstor.layer.cache;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.layer.AbsCacheLayerSizeCalculator;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CacheLayerSizeCalculator extends AbsCacheLayerSizeCalculator<CacheVlmData<?>>
{
    public static final String DFLT_CACHE_SIZE = "5%";
    public static final String DFLT_META_SIZE = "12288"; // 12M

    @Inject
    public CacheLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(
            initRef,
            DeviceLayerKind.CACHE,
            ApiConsts.NAMESPC_CACHE,
            0,
            new CacheDeviceInfo(
                RscLayerSuffixes.SUFFIX_CACHE_CACHE,
                ApiConsts.KEY_CACHE_CACHE_SIZE,
                DFLT_CACHE_SIZE
            ),
            new CacheDeviceInfo(
                RscLayerSuffixes.SUFFIX_CACHE_META,
                ApiConsts.KEY_CACHE_META_SIZE,
                DFLT_META_SIZE
            )
        );
    }
}
