package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSnapshotVlmDfnDriver
    extends AbsSatelliteDbDriver<SnapshotVolumeDefinition>
    implements SnapshotVolumeDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<SnapshotVolumeDefinition> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> volumeSizeDriver;

    @Inject
    public SatelliteSnapshotVlmDfnDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        volumeSizeDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> getVolumeSizeDriver()
    {
        return volumeSizeDriver;
    }

    @Override
    public StateFlagsPersistence<SnapshotVolumeDefinition> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }
}
