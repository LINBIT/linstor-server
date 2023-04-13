package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerNvmeRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;

import javax.inject.Inject;

public class SatelliteLayerNvmeRscDbDriver implements LayerNvmeRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerNvmeRscDbDriver()
    {
    }


    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void create(NvmeRscData<?> nvmeRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(NvmeRscData<?> nvmeRscDataRef)
    {
        // no-op
    }
}
