package com.linbit.linstor.dbdrivers;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteSnapshotDfnDriver implements SnapshotDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteSnapshotDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<SnapshotDefinition> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<SnapshotDefinition>) stateFlagsDriver;
    }

    @Override
    public void create(SnapshotDefinition snapshotDefinition)
    {
        // no-op
    }

    @Override
    public void delete(SnapshotDefinition snapshotDefinition)
    {
        // no-op
    }
}
