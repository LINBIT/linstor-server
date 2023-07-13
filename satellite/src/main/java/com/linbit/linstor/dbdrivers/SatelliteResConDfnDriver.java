package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteResConDfnDriver
    extends AbsSatelliteDbDriver<ResourceConnection>
    implements ResourceConnectionDatabaseDriver
{
    private final StateFlagsPersistence<ResourceConnection> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> portDriver;

    @Inject
    public SatelliteResConDfnDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        portDriver = getNoopColumnDriver();
    }

    @Override
    public StateFlagsPersistence<ResourceConnection> getStateFlagPersistence()
    {
        return stateFlagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> getPortDriver()
    {
        return portDriver;
    }
}
