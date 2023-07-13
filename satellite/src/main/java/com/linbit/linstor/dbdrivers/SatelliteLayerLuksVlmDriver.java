package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;

import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
public class SatelliteLayerLuksVlmDriver
    extends AbsSatelliteDbDriver<LuksVlmData<?>>
    implements LayerLuksVlmDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;
    private final SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> noopPwDriver;

    @Inject
    public SatelliteLayerLuksVlmDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
        noopPwDriver = getNoopColumnDriver();
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver()
    {
        return noopPwDriver;
    }
}
