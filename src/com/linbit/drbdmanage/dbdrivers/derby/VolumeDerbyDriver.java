package com.linbit.drbdmanage.dbdrivers.derby;

import java.sql.Connection;

import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class VolumeDerbyDriver implements VolumeDatabaseDriver
{

    public VolumeDerbyDriver(Volume volume)
    {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void setConnection(Connection dbCon)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
