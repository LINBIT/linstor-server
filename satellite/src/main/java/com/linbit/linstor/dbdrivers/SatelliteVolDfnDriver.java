package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteVolDfnDriver
    extends AbsSatelliteDbDriver<VolumeDefinition>
    implements VolumeDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<VolumeDefinition> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<VolumeDefinition, Long> volumeSizeDriver;

    @Inject
    public SatelliteVolDfnDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        volumeSizeDriver = getNoopColumnDriver();
    }

    @Override
    public StateFlagsPersistence<VolumeDefinition> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<VolumeDefinition, Long> getVolumeSizeDriver()
    {
        return volumeSizeDriver;
    }
}
