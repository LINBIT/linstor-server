package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;

import java.util.UUID;

public interface LayerBCacheVlmDatabaseDriver extends AbsLayerDataDatabaseDriver<BCacheVlmData<?>>
{
    // BCacheVlmData do have an additional storage pool, but that cannot be changed, so no special driver for that

    SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver();
}
