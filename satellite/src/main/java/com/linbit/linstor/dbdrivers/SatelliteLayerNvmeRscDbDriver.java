package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerNvmeRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerNvmeRscDbDriver
    extends AbsSatelliteDbDriver<NvmeRscData<?>>
    implements LayerNvmeRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;

    @Inject
    public SatelliteLayerNvmeRscDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}
