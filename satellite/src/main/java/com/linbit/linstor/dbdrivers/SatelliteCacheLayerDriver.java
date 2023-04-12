package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.CacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;

import javax.inject.Inject;

public class SatelliteCacheLayerDriver implements CacheLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteCacheLayerDriver()
    {
    }


    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void persist(CacheRscData<?> cacheRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(CacheRscData<?> cacheRscDataRef)
    {
        // no-op
    }

    @Override
    public void persist(CacheVlmData<?> cacheVlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(CacheVlmData<?> cacheVlmDataRef)
    {
        // no-op
    }
}
