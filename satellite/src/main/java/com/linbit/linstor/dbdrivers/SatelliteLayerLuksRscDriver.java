package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerLuksRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;

import javax.inject.Inject;

public class SatelliteLayerLuksRscDriver implements LayerLuksRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerLuksRscDriver()
    {
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void create(LuksRscData<?> luksRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(LuksRscData<?> luksRscDataRef)
    {
        // no-op
    }
}
