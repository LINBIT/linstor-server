package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteSnapshotVlmDriver implements SnapshotVolumeDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteSnapshotVlmDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public void create(SnapshotVolume snapshot)
    {
        // no-op
    }

    @Override
    public void delete(SnapshotVolume snapshot)
    {
        // no-op
    }
}
