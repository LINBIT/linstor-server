package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerOpenflexRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;

import javax.inject.Inject;

public class SatelliteLayerOpenflexRscDbDriver implements LayerOpenflexRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerOpenflexRscDbDriver()
    {
    }

    @Override
    public void create(OpenflexRscData<?> openflexRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(OpenflexRscData<?> openflexRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

