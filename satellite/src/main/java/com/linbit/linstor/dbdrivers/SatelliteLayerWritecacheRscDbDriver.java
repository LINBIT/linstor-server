package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheRscDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;

import javax.inject.Inject;

public class SatelliteLayerWritecacheRscDbDriver implements LayerWritecacheRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerWritecacheRscDbDriver()
    {
    }

    @Override
    public void create(WritecacheRscData<?> writecacheRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(WritecacheRscData<?> writecacheRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

