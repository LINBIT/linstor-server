package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;

import javax.inject.Inject;

public class SatelliteLayerCacheVlmDbDriver implements LayerCacheVlmDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerCacheVlmDbDriver()
    {
    }

    @Override
    public void create(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

