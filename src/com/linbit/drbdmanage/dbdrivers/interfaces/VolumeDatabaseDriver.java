package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;

import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDatabaseDriver
{
    void setConnection(Connection dbCon);

    StateFlagsPersistence getStateFlagsPersistence();
}
