package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDefinitionDataDatabaseDriver
{
    StateFlagsPersistence getStateFlagsPersistence();

    ObjectDatabaseDriver<MinorNumber> getMinorNumberDriver();

    ObjectDatabaseDriver<Long> getVolumeSizeDriver();

    VolumeDefinitionData load(Connection con, SerialGenerator serialGen) throws SQLException;
}
