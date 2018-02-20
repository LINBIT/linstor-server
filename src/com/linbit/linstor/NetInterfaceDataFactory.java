package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class NetInterfaceDataFactory
{
    private final NetInterfaceDataDatabaseDriver driver;

    @Inject
    public NetInterfaceDataFactory(NetInterfaceDataDatabaseDriver driverRef)
    {
        driver = driverRef;
    }

    public NetInterfaceData getInstance(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        LsIpAddress addr,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        NetInterfaceData netData = null;

        netData = driver.load(node, name, false, transMgr);

        if (failIfExists && netData != null)
        {
            throw new LinStorDataAlreadyExistsException("The NetInterface already exists");
        }

        if (netData == null && createIfNotExists)
        {
            netData = new NetInterfaceData(UUID.randomUUID(), accCtx, name, node, addr, driver);
            netData.setConnection(transMgr);
            driver.create(netData, transMgr);
        }
        if (netData != null)
        {
            netData.initialized();
        }

        return netData;
    }

    public NetInterfaceData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {

        NetInterfaceData netData;
        try
        {
            netData = driver.load(node, netName, false, transMgr);
            if (netData == null)
            {
                netData = new NetInterfaceData(
                    uuid,
                    accCtx,
                    netName,
                    node,
                    addr,
                    driver
                );
            }
            netData.initialized();
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return netData;
    }
}
