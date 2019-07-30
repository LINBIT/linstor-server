package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.NetInterfaceData;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import javax.inject.Inject;

public class SatelliteNiDriver implements NetInterfaceDataDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteNiDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> getNetInterfaceAddressDriver()
    {
        return (SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType> getStltConnEncrTypeDriver()
    {
        return (SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber> getStltConnPortDriver()
    {
        return (SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber>) singleColDriver;
    }

    @Override
    public void create(NetInterfaceData netInterfaceData)
    {
        // no-op
    }

    @Override
    public void delete(NetInterfaceData data)
    {
        // no-op
    }
}
