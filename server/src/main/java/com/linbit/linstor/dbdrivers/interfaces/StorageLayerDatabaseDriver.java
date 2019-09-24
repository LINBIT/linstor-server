package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public interface StorageLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(StorageRscData storageRscDataRef) throws DatabaseException;
    void delete(StorageRscData storgeRscDataRef) throws DatabaseException;

    void persist(VlmProviderObject vlmDataRef) throws DatabaseException;
    void delete(VlmProviderObject vlmDataRef) throws DatabaseException;

    SingleColumnDatabaseDriver<VlmProviderObject, StorPool> getStorPoolDriver();
}
