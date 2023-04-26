package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;

import javax.inject.Inject;

public class SatelliteLayerBCacheRscDbDriver implements LayerBCacheRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerBCacheRscDbDriver()
    {
    }

    @Override
    public void create(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

