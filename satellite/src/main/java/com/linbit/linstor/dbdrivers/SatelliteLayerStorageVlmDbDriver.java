package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;

public class SatelliteLayerStorageVlmDbDriver implements LayerStorageVlmDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();

    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();
    private final SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> noopStorPoolDriver;

    @SuppressWarnings("unchecked")
    @Inject
    public SatelliteLayerStorageVlmDbDriver()
    {
        noopStorPoolDriver = (SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool>) noopSingleColDriver;
    }

    @Override
    public void create(VlmProviderObject<?> vlmDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(VlmProviderObject<?> vlmDataRef) throws DatabaseException
    {
        // no-op
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

