package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.sql.SQLException;

public interface StorageLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(StorageRscData storageRscDataRef) throws SQLException;
    void delete(StorageRscData storgeRscDataRef) throws SQLException;

    void persist(VlmProviderObject vlmDataRef) throws SQLException;
    void delete(VlmProviderObject vlmDataRef) throws SQLException;

    SingleColumnDatabaseDriver<VlmProviderObject, StorPool> getStorPoolDriver();
}
