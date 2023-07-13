package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerDrbdVlmDbDriver
    extends AbsSatelliteDbDriver<DrbdVlmData<?>>
    implements LayerDrbdVlmDatabaseDriver
{
    private final SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> extStorPoolDriver;
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;

    @Inject
    public SatelliteLayerDrbdVlmDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
        extStorPoolDriver = getNoopColumnDriver();
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> getExtStorPoolDriver()
    {
        return extStorPoolDriver;
    }

}
