package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class SatelliteLayerBCacheVlmDbDriver
    extends AbsSatelliteDbDriver<BCacheVlmData<?>>
    implements LayerBCacheVlmDatabaseDriver
{
    private final SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> noopDeviceUuidDriver;
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;

    @Inject
    public SatelliteLayerBCacheVlmDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
        noopDeviceUuidDriver = getNoopColumnDriver();
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
