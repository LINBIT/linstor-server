package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.layer.data.RaidData;

public interface RaidDatabaseDriver
{
    SingleColumnDatabaseDriver<RaidData, Integer> getLevelDriver();

}
