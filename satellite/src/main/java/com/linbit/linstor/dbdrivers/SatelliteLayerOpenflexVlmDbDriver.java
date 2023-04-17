package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerOpenflexVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;

import javax.inject.Inject;

public class SatelliteLayerOpenflexVlmDbDriver implements LayerOpenflexVlmDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerOpenflexVlmDbDriver()
    {
    }

    @Override
    public void create(OpenflexVlmData<?> openflexVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(OpenflexVlmData<?> openflexVlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

