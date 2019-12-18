package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;

import javax.inject.Inject;

public class SatelliteWritecacheLayerDriver implements WritecacheLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final ResourceLayerIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteWritecacheLayerDriver()
    {
    }


    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void persist(WritecacheRscData<?> writecacheRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(WritecacheRscData<?> writecacheRscDataRef)
    {
        // no-op
    }

    @Override
    public void persist(WritecacheVlmData<?> writecacheVlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(WritecacheVlmData<?> writecacheVlmDataRef)
    {
        // no-op
    }
}
