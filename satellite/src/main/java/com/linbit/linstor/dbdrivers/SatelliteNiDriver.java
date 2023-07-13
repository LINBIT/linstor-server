package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteNiDriver
    extends AbsSatelliteDbDriver<NetInterface>
    implements NetInterfaceDatabaseDriver
{
    private final SingleColumnDatabaseDriver<NetInterface, LsIpAddress> netIfAddrDriver;
    private final SingleColumnDatabaseDriver<NetInterface, EncryptionType> stltConnEncrTypeDriver;
    private final SingleColumnDatabaseDriver<NetInterface, TcpPortNumber> stltConnPortDriver;

    @Inject
    public SatelliteNiDriver()
    {
        netIfAddrDriver = getNoopColumnDriver();
        stltConnEncrTypeDriver = getNoopColumnDriver();
        stltConnPortDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterface, LsIpAddress> getNetInterfaceAddressDriver()
    {
        return netIfAddrDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterface, EncryptionType> getStltConnEncrTypeDriver()
    {
        return stltConnEncrTypeDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterface, TcpPortNumber> getStltConnPortDriver()
    {
        return stltConnPortDriver;
    }
}
