package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.Node;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import java.sql.SQLException;

public class SatelliteSnapshotDriver implements SnapshotDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteSnapshotDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<Snapshot> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<Snapshot>) stateFlagsDriver;
    }

    @Override
    public void create(Snapshot snapshot)
        throws SQLException
    {
        // no-op
    }

    @Override
    public Snapshot load(
        Node node, SnapshotDefinition snapshotDefinition, boolean logWarnIfNotExists
    )
        throws SQLException
    {
        return snapshotDefinition.getSnapshot(node.getName());
    }

    @Override
    public void delete(Snapshot snapshot)
        throws SQLException
    {
        // no-op
    }
}
