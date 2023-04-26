package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheVlmDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;

import javax.inject.Inject;

public class SatelliteLayerWritecacheVlmDbDriver implements LayerWritecacheVlmDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerWritecacheVlmDbDriver()
    {
    }

    @Override
    public void create(WritecacheVlmData<?> writecacheVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(WritecacheVlmData<?> writecacheVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

