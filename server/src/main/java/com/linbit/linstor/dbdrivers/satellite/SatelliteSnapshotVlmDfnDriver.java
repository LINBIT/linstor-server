package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteSnapshotVlmDfnDriver implements SnapshotVolumeDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> volumeSizeDriver =
        new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteSnapshotVlmDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
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
