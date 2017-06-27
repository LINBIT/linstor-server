package com.linbit.drbdmanage;

import com.linbit.MapDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class ResourceDataDerbyDriver implements ResourceDataDatabaseDriver
{

    public ResourceDataDerbyDriver(Resource res)
    {
        // TODO Auto-generated constructor stub
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
