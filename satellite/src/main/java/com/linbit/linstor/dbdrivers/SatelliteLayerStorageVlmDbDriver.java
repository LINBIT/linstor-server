package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerStorageVlmDbDriver
    extends AbsSatelliteDbDriver<VlmProviderObject<?>>
    implements LayerStorageVlmDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;
    private final SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> noopStorPoolDriver;

    @Inject
    public SatelliteLayerStorageVlmDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
        noopStorPoolDriver = getNoopColumnDriver();
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> getStorPoolDriver()
    {
        return noopStorPoolDriver;
    }
}
