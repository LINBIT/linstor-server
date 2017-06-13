package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDefinitionDatabaseDriver
{
    void setConnection(Connection con);

    StateFlagsPersistence getStateFlagsPersistence();

    ObjectDatabaseDriver<MinorNumber> getMinorNumberDriver();

    ObjectDatabaseDriver<Long> getVolumeSizeDriver();
}
