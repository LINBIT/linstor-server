package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

import java.util.Date;

public class SatelliteSnapshotDriver implements SnapshotDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteSnapshotDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<AbsResource<Snapshot>> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<AbsResource<Snapshot>>) stateFlagsDriver;
    }

    @Override
    public void create(AbsResource<Snapshot> snapshot)
    {
        // no-op
    }

    @Override
    public void delete(AbsResource<Snapshot> snapshot)
    {
        // no-op
    }

    @Override
    public SingleColumnDatabaseDriver<AbsResource<Snapshot>, Date> getCreateTimeDriver()
    {
        return (SingleColumnDatabaseDriver<AbsResource<Snapshot>, Date>) noopSingleColDriver;
    }
}
