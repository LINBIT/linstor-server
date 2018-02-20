package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.TransactionMgr;
import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteVolDriver implements VolumeDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();

    @Inject
    public SatelliteVolDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<VolumeData> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<VolumeData>) stateFlagsDriver;
    }

    @Override
    public VolumeData load(
        Resource resource,
        VolumeDefinition volumeDefinition,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
    {
        return (VolumeData) resource.getVolume(volumeDefinition.getVolumeNumber());
    }

    @Override
    public void create(VolumeData vol, TransactionMgr transMgr)
    {
        // no-op
    }

    @Override
    public void delete(VolumeData data, TransactionMgr transMgr)
    {
        // no-op
    }
}
