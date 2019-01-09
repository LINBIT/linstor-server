package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDfnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject;

import javax.inject.Inject;

public class SatelliteDrbdRscDfnDriver implements DrbdRscDfnDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteDrbdRscDfnDriver()
    {
    }


    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnObject, TcpPortNumber> getPortDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnObject, TcpPortNumber>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnObject, TransportType> getTransportTypeDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnObject, TransportType>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnObject, String> getSecretDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnObject, String>) singleColDriver;
    }
}
