package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSnapshotDfnDriver
    extends AbsSatelliteDbDriver<SnapshotDefinition>
    implements SnapshotDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<SnapshotDefinition> stateFlagsDriver;

    @Inject
    public SatelliteSnapshotDfnDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
    }

    @Override
    public StateFlagsPersistence<SnapshotDefinition> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }
}
