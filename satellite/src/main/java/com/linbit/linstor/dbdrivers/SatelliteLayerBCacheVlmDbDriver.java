package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;

import javax.inject.Inject;

import java.util.UUID;

public class SatelliteLayerBCacheVlmDbDriver implements LayerBCacheVlmDatabaseDriver
{
    private final SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> noopDeviceUuidDriver;
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerBCacheVlmDbDriver()
    {
        noopDeviceUuidDriver = new SatelliteSingleColDriver<>();
    }

    @Override
    public void create(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver()
    {
        return noopDeviceUuidDriver;
    }
}

