package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.SatelliteConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class SatelliteConnectionDataFactory
{
    private final SatelliteConnectionDataDatabaseDriver driver;

    @Inject
    public SatelliteConnectionDataFactory(SatelliteConnectionDataDatabaseDriver driverRef)
    {
        driver = driverRef;
    }

    public SatelliteConnection getInstance(
        AccessContext accCtx,
        Node node,
        NetInterface netIf,
        TcpPortNumber port,
        SatelliteConnection.EncryptionType encryptionType,
        TransactionMgr transMgr,
        boolean createIfNotExits,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        SatelliteConnectionData stltConn = driver.load(node, false, transMgr);

        if (failIfExists && stltConn != null)
        {
            throw new LinStorDataAlreadyExistsException("The satellite connection already exists");
        }

        if (createIfNotExits && stltConn == null)
        {
            stltConn = new SatelliteConnectionData(UUID.randomUUID(), node, netIf, port, encryptionType, driver);
            driver.create(stltConn, transMgr);

            node.setSatelliteConnection(accCtx, stltConn);
        }

        if (stltConn != null)
        {
            stltConn.initialized();
            stltConn.setConnection(transMgr);
        }

        return stltConn;
    }

    public SatelliteConnectionData getInstanceSatellite(
        UUID uuid,
        Node node,
        NetInterface netIf,
        TcpPortNumber port,
        SatelliteConnection.EncryptionType encryptionType,
        SatelliteTransactionMgr transMgr
    )
    {
        SatelliteConnectionData stltConn;
        try
        {
            stltConn = driver.load(node, false, transMgr);
            if (stltConn == null)
            {
                stltConn = new SatelliteConnectionData(uuid, node, netIf, port, encryptionType, driver);
            }
            stltConn.initialized();
            stltConn.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return stltConn;
    }
}
