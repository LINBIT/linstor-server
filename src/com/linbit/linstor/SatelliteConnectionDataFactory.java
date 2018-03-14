package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SatelliteConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class SatelliteConnectionDataFactory
{
    private final SatelliteConnectionDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SatelliteConnectionDataFactory(
        SatelliteConnectionDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SatelliteConnection getInstance(
        AccessContext accCtx,
        Node node,
        NetInterface netIf,
        TcpPortNumber port,
        SatelliteConnection.EncryptionType encryptionType,
        boolean createIfNotExits,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        SatelliteConnectionData stltConn = driver.load(node, false);

        if (failIfExists && stltConn != null)
        {
            throw new LinStorDataAlreadyExistsException("The satellite connection already exists");
        }

        if (createIfNotExits && stltConn == null)
        {
            stltConn = new SatelliteConnectionData(
                UUID.randomUUID(),
                node,
                netIf,
                port,
                encryptionType,
                driver,
                transObjFactory,
                transMgrProvider
            );
            driver.create(stltConn);

            node.setSatelliteConnection(accCtx, stltConn);
        }

        return stltConn;
    }

    public SatelliteConnectionData getInstanceSatellite(
        UUID uuid,
        Node node,
        NetInterface netIf,
        TcpPortNumber port,
        SatelliteConnection.EncryptionType encryptionType
    )
    {
        SatelliteConnectionData stltConn;
        try
        {
            stltConn = driver.load(node, false);
            if (stltConn == null)
            {
                stltConn = new SatelliteConnectionData(
                    uuid,
                    node,
                    netIf,
                    port,
                    encryptionType,
                    driver,
                    transObjFactory,
                    transMgrProvider
                );
            }
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
