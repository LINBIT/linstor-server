package com.linbit.linstor.dbdrivers;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;

public class SatelliteNiDriver implements NetInterfaceDatabaseDriver
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
    public SingleColumnDatabaseDriver<NetInterface, LsIpAddress> getNetInterfaceAddressDriver()
    {
        return (SingleColumnDatabaseDriver<NetInterface, LsIpAddress>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<NetInterface, EncryptionType> getStltConnEncrTypeDriver()
    {
        return (SingleColumnDatabaseDriver<NetInterface, EncryptionType>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<NetInterface, TcpPortNumber> getStltConnPortDriver()
    {
        return (SingleColumnDatabaseDriver<NetInterface, TcpPortNumber>) singleColDriver;
    }

    @Override
    public void create(NetInterface netInterface)
    {
        // no-op
    }

    @Override
    public void delete(NetInterface data)
    {
        // no-op
    }
}
