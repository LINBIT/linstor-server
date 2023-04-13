package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;

import javax.inject.Inject;

public class SatelliteLayerLuksVlmDriver implements LayerLuksVlmDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();
    private final SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> noopPwDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteLayerLuksVlmDriver()
    {
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void create(LuksVlmData<?> dataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(LuksVlmData<?> dataRef) throws DatabaseException
    {
        // no-op

    }

    @Override
    public SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver()
    {
        return noopPwDriver;
    }
}
