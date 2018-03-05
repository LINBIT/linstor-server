package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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

    @Override
    public NetInterfaceData load(
        Node node,
        NetInterfaceName netInterfaceName,
        boolean logWarnIfNotExists
    )

    {
        NetInterfaceData netInterface = null;
        try
        {
            netInterface = (NetInterfaceData) node.getNetInterface(dbCtx, netInterfaceName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            SatelliteDbDriverExceptionHandler.handleAccessDeniedException(accDeniedExc);
        }
        return netInterface;
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
