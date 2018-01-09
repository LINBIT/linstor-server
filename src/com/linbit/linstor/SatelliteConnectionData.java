package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.SatelliteConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

public class SatelliteConnectionData extends BaseTransactionObject implements SatelliteConnection
{
    // Object identifier
    private final UUID objId;

    private final Node node;

    private final NetInterface netIf;

    private final TransactionSimpleObject<SatelliteConnectionData, TcpPortNumber> port;

    private final TransactionSimpleObject<SatelliteConnectionData, EncryptionType> encryptionType;

    private final SatelliteConnectionDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    private SatelliteConnectionData(
        Node node,
        NetInterface netIf,
        TcpPortNumber port,
        EncryptionType encryptionType
    )
    {
        this (
            UUID.randomUUID(),
            node,
            netIf,
            port,
            encryptionType
        );
    }

    SatelliteConnectionData(
        UUID uuid,
        Node nodeRef,
        NetInterface netIfRef,
        TcpPortNumber portRef,
        EncryptionType encryptionTypeRef
    )
    {
        ErrorCheck.ctorNotNull(SatelliteConnectionData.class, Node.class, nodeRef);
        ErrorCheck.ctorNotNull(SatelliteConnectionData.class, NetInterface.class, netIfRef);

        objId = uuid;
        node = nodeRef;
        netIf = netIfRef;

        dbDriver = LinStor.getSatelliteConnectionDataDatabaseDriver();

        port = new TransactionSimpleObject<>(this, portRef, dbDriver.getSatelliteConnectionPortDriver());
        encryptionType = new TransactionSimpleObject<>(this, encryptionTypeRef, dbDriver.getSatelliteConnectionTypeDriver());

        transObjs = Arrays.asList(
            node,
            netIf,
            port,
            encryptionType
        );
    }

    public static SatelliteConnection getInstance(
        AccessContext accCtx,
        Node node,
        NetInterface netIf,
        TcpPortNumber port,
        EncryptionType encryptionType,
        TransactionMgr transMgr,
        boolean createIfNotExits,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        SatelliteConnectionDataDatabaseDriver driver = LinStor.getSatelliteConnectionDataDatabaseDriver();
        SatelliteConnectionData stltConn = driver.load(node, false, transMgr);

        if (failIfExists && stltConn != null)
        {
            throw new LinStorDataAlreadyExistsException("The satellite connection already exists");
        }

        if (createIfNotExits && stltConn == null)
        {
            stltConn = new SatelliteConnectionData(node, netIf, port, encryptionType);
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

    public static SatelliteConnection getInstanceSatellite(
        UUID uuid,
        Node node,
        NetInterface netIf,
        TcpPortNumber port,
        EncryptionType encryptionType,
        SatelliteTransactionMgr transMgr
    )
    {
        SatelliteConnectionDataDatabaseDriver driver = LinStor.getSatelliteConnectionDataDatabaseDriver();
        SatelliteConnectionData stltConn;
        try
        {
            stltConn = driver.load(node, false, transMgr);
            if (stltConn == null)
            {
                stltConn = new SatelliteConnectionData(uuid, node, netIf, port, encryptionType);
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

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public Node getNode()
    {
        checkDeleted();
        return node;
    }

    @Override
    public NetInterface getNetInterface()
    {
        checkDeleted();
        return netIf;
    }

    @Override
    public TcpPortNumber getPort()
    {
        checkDeleted();
        return port.get();
    }

    @Override
    public void setPort(AccessContext accCtx, TcpPortNumber newPort)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        port.set(newPort);
    }

    @Override
    public EncryptionType getEncryptionType()
    {
        checkDeleted();
        return encryptionType.get();
    }

    @Override
    public void setEncryptionType(AccessContext accCtx, EncryptionType newEncryptionType)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        encryptionType.set(newEncryptionType);
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted satellite connection", null);
        }
    }
}
