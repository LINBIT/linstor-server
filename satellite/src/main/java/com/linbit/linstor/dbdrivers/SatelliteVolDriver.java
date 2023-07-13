package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteVolDriver
    extends AbsSatelliteDbDriver<Volume>
    implements VolumeDatabaseDriver
{
    private final StateFlagsPersistence<Volume> stateFlagsDriver;

    @Inject
    public SatelliteVolDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
    }

    @Override
    public StateFlagsPersistence<Volume> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }
}
