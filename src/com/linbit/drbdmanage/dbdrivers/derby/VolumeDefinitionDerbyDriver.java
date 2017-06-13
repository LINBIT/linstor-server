package com.linbit.drbdmanage.dbdrivers.derby;

import java.sql.Connection;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class VolumeDefinitionDerbyDriver implements VolumeDefinitionDatabaseDriver
{

    public VolumeDefinitionDerbyDriver(VolumeDefinition volumeDefinition)
    {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void setConnection(Connection con)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectDatabaseDriver<MinorNumber> getMinorNumberDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectDatabaseDriver<Long> getVolumeSizeDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
