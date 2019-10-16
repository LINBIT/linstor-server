package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public interface ResourceLayerIdDatabaseDriver
{
    void persist(AbsRscLayerObject<?> rscLayerObject) throws DatabaseException;

    void delete(AbsRscLayerObject<?> rscLayerObject) throws DatabaseException;

    <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
    SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> getParentDriver();
}
