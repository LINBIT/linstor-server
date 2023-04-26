package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;

public interface LayerCacheVlmDatabaseDriver extends AbsLayerDataDatabaseDriver<CacheVlmData<?>>
{
    // CacheVlmData do have an additional storage pool, but that cannot be changed, so no special driver for that
}
