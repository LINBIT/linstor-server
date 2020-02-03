package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteSnapshotDriver implements SnapshotDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();

    @Inject
    public SatelliteSnapshotDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<Snapshot> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<Snapshot>) stateFlagsDriver;
    }

    @Override
    public void create(Snapshot snapshot)
    {
        // no-op
    }

    @Override
    public void delete(Snapshot snapshot)
    {
        // no-op
    }
}
