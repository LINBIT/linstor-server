package com.linbit.drbdmanage.dbdrivers.interfaces;

import com.linbit.MapDatabaseDriver;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface ResourceDataDatabaseDriver
{
    MapDatabaseDriver<VolumeNumber, Volume> getVolumeMapDriver();

    StateFlagsPersistence getStateFlagPersistence();
}
