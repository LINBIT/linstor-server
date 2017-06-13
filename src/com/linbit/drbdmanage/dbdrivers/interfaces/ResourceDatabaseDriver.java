package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;

import com.linbit.MapDatabaseDriver;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface ResourceDatabaseDriver
{
    void setConnection(Connection dbCon);

    MapDatabaseDriver<VolumeNumber, Volume> getVolumeMapDriver();

    StateFlagsPersistence getStateFlagPersistence();
}
