package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Instant;

@Singleton
public class SatelliteSnapshotDriver
    extends AbsSatelliteDbDriver<AbsResource<Snapshot>>
    implements SnapshotDatabaseDriver
{
    private final StateFlagsPersistence<AbsResource<Snapshot>> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<AbsResource<Snapshot>, Instant> createTimeDriver;

    @Inject
    public SatelliteSnapshotDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        createTimeDriver = getNoopColumnDriver();
    }

    @Override
    public StateFlagsPersistence<AbsResource<Snapshot>> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<AbsResource<Snapshot>, Instant> getCreateTimeDriver()
    {
        return createTimeDriver;
    }
}
