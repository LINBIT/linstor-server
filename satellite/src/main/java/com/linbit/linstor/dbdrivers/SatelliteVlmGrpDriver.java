package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteVlmGrpDriver
    extends AbsSatelliteDbDriver<VolumeGroup>
    implements VolumeGroupDatabaseDriver
{
    private final StateFlagsPersistence<VolumeGroup> stateFlagsDriver;

    @Inject
    public SatelliteVlmGrpDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
    }

    @Override
    public StateFlagsPersistence<VolumeGroup> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }
}
