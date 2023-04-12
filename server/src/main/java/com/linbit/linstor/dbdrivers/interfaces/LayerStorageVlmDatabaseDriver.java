package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public interface LayerStorageVlmDatabaseDriver extends AbsLayerDataDatabaseDriver<VlmProviderObject<?>>
{
    SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> getStorPoolDriver();
}
