package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;

@Singleton
public class SatelliteStorageLayerDriver implements StorageLayerDatabaseDriver
{
    private static final SatelliteResourceLayerIdDriver NO_OP_ID_DRIVER = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteStorageLayerDriver()
    {
    }

    @Override
    public void persist(StorageRscData storageRscDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(StorageRscData storgeRscDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void persist(VlmProviderObject vlmDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(VlmProviderObject vlmDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return NO_OP_ID_DRIVER;
    }
}
