package com.linbit.linstor.layer.writecache;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.layer.AbsCacheLayerSizeCalculator;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class WritecacheLayerSizeCalculator extends AbsCacheLayerSizeCalculator<WritecacheVlmData<Resource>>
{
    public static final String DFLT_CACHE_SIZE = "5%";

    @Inject
    public WritecacheLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.WRITECACHE, ApiConsts.NAMESPC_WRITECACHE, 0);
        registerChildSizeCalculator(
            RscLayerSuffixes.SUFFIX_WRITECACHE_CACHE,
            ApiConsts.KEY_WRITECACHE_SIZE,
            DFLT_CACHE_SIZE
        );
    }
}
