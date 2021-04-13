package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteSnapshotVlmDfnDriver implements SnapshotVolumeDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> volumeSizeDriver =
        new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteSnapshotVlmDfnDriver()
    {
    }

    @Override
    public void create(SnapshotVolumeDefinition snapshotVlmDfn)
    {
        // no-op
    }

    @Override
    public void delete(SnapshotVolumeDefinition snapshotVlmDfn)
    {
        // no-op
    }

    @Override
    public SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> getVolumeSizeDriver()
    {
        return volumeSizeDriver;
    }

    @Override
    public StateFlagsPersistence<SnapshotVolumeDefinition> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<SnapshotVolumeDefinition>) stateFlagsDriver;
    }
}
