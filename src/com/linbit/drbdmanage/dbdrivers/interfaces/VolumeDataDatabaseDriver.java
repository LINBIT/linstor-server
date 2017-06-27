package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagsPersistence();

    public VolumeData load(Connection dbCon, Resource resRef, VolumeDefinition volDfn)
        throws SQLException;

    public void create(Connection dbCon, VolumeData vol)
        throws SQLException;
}
