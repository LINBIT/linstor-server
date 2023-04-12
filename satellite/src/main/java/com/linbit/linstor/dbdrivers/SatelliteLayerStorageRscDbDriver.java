package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.storage.data.provider.StorageRscData;

import javax.inject.Inject;

public class SatelliteLayerStorageRscDbDriver implements LayerStorageRscDatabaseDriver
{
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerStorageRscDbDriver()
    {
    }

    @Override
    public void create(StorageRscData<?> drbdRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(StorageRscData<?> drbdRscDataRef) throws DatabaseException
    {
        // no-op
    }


    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

