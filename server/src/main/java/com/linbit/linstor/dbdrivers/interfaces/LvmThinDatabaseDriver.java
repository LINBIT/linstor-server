package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.layer.data.LvmThinLayerData;

public interface LvmThinDatabaseDriver
{
    SingleColumnDatabaseDriver<LvmThinLayerData, String> getVolumeGroupDriver();

    SingleColumnDatabaseDriver<LvmThinLayerData, String> getThinPoolDriver();

    SingleColumnDatabaseDriver<LvmThinLayerData, Double> getToleranceFactorDriver();
}
