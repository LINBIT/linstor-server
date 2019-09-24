package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public interface ResourceLayerIdDatabaseDriver
{
    void persist(RscLayerObject rscLayerObject) throws DatabaseException;

    void delete(RscLayerObject rscLayerObject) throws DatabaseException;

    <T extends VlmProviderObject> SingleColumnDatabaseDriver<AbsRscData<T>, RscLayerObject> getParentDriver();
}
