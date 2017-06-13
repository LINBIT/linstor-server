package com.linbit.drbdmanage.dbdrivers.derby;

import java.sql.Connection;

import com.linbit.MapDatabaseDriver;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class ResourceDerbyDriver implements ResourceDatabaseDriver
{

    public ResourceDerbyDriver(Resource res)
    {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void setConnection(Connection dbCon)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public MapDatabaseDriver<VolumeNumber, Volume> getVolumeMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StateFlagsPersistence getStateFlagPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
