package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerCacheVlmDbDriver
    extends AbsSatelliteDbDriver<CacheVlmData<?>>
    implements LayerCacheVlmDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;

    @Inject
    public SatelliteLayerCacheVlmDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}
