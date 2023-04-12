package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;

public interface LayerDrbdVlmDatabaseDriver extends AbsLayerDataDatabaseDriver<DrbdVlmData<?>>
{
    SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> getExtStorPoolDriver();
}
