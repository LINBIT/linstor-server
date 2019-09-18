package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteResConDfnDriver implements ResourceConnectionDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteResConDfnDriver()
    {
    }

    @Override
    public void create(ResourceConnection conDfnData)
    {
        // no-op
    }

    @Override
    public void delete(ResourceConnection data)
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<ResourceConnection> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<ResourceConnection>) stateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> getPortDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber>) singleColDriver;
    }
}
