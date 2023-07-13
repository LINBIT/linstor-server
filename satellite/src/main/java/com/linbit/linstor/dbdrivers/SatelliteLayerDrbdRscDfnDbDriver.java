package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerDrbdRscDfnDbDriver
    extends AbsSatelliteDbDriver<DrbdRscDfnData<?>> implements LayerDrbdRscDfnDatabaseDriver
{
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> tcpPortDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> transportTypeDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> secretDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> peerSlotsDriver;

    @Inject
    public SatelliteLayerDrbdRscDfnDbDriver()
    {
        tcpPortDriver = getNoopColumnDriver();
        transportTypeDriver = getNoopColumnDriver();
        secretDriver = getNoopColumnDriver();
        peerSlotsDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> getTcpPortDriver()
    {
        return tcpPortDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> getTransportTypeDriver()
    {
        return transportTypeDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> getRscDfnSecretDriver()
    {
        return secretDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> getPeerSlotsDriver()
    {
        return peerSlotsDriver;
    }
}
