package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;

import javax.inject.Inject;

public class SatelliteLayerCacheRscDbDriver
    extends AbsSatelliteDbDriver<CacheRscData<?>>
    implements LayerCacheRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;

    @Inject
    public SatelliteLayerCacheRscDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}
