package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;

import javax.inject.Inject;

public class SatelliteLayerDrbdVlmDbDriver implements LayerDrbdVlmDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerDrbdVlmDbDriver()
    {
    }

    @Override
    public void create(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> getExtStorPoolDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool>) noopSingleColDriver;
    }

}

