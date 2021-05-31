package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;

import javax.inject.Inject;

import java.util.UUID;

public class SatelliteBCacheLayerDriver implements BCacheLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final ResourceLayerIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteBCacheLayerDriver()
    {
    }


    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver()
    {
        return (SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID>) noopSingleColDriver;
    }

    @Override
    public void persist(BCacheRscData<?> bcacheRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(BCacheRscData<?> bcacheRscDataRef)
    {
        // no-op
    }

    @Override
    public void persist(BCacheVlmData<?> bcacheVlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(BCacheVlmData<?> bcacheVlmDataRef)
    {
        // no-op
    }
}
