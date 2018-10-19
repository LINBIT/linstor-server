package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteResConDfnDriver implements ResourceConnectionDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteResConDfnDriver()
    {
    }

    @Override
    public void create(ResourceConnectionData conDfnData)
    {
        // no-op
    }

    @Override
    public void delete(ResourceConnectionData data)
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<ResourceConnectionData> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<ResourceConnectionData>) stateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber> getPortDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber>) singleColDriver;
    }
}
