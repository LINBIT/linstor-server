package com.linbit.drbdmanage.dbdrivers.interfaces;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDefinitionDataDatabaseDriver
{
    StateFlagsPersistence getStateFlagsPersistence();

    ObjectDatabaseDriver<MinorNumber> getMinorNumberDriver();

    ObjectDatabaseDriver<Long> getVolumeSizeDriver();
}
