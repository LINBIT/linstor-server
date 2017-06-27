package com.linbit.drbdmanage;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class VolumeDataDefinitionDerbyDriver implements VolumeDefinitionDataDatabaseDriver
{

    public VolumeDataDefinitionDerbyDriver(VolumeDefinition volumeDefinition)
    {
        // TODO Auto-generated constructor stub
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
