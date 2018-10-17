package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDfnDatabaseDriver;
import com.linbit.linstor.storage2.layer.data.DrbdRscDfnData;

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
    public SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber> getPortDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType> getTransportTypeDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, String> getSecretDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData, String>) singleColDriver;
    }
}
