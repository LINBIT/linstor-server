package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheRscDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerWritecacheRscDbDriver
    extends AbsSatelliteDbDriver<WritecacheRscData<?>>
    implements LayerWritecacheRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;

    @Inject
    public SatelliteLayerWritecacheRscDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}
