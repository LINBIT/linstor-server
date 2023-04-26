package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;

import javax.inject.Inject;

public class SatelliteLayerCacheRscDbDriver implements LayerCacheRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerCacheRscDbDriver()
    {
    }

    @Override
    public void create(CacheRscData<?> cacheRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(CacheRscData<?> cacheRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

