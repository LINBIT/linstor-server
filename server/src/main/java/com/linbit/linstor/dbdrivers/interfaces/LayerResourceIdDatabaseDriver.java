package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public interface LayerResourceIdDatabaseDriver extends GenericDatabaseDriver<AbsRscLayerObject<?>>
{
    <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
    SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> getParentDriver();

    <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
    SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean> getSuspendDriver();
}
