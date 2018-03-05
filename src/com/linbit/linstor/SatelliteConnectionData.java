package com.linbit.linstor;

import com.linbit.ErrorCheck;
import com.linbit.linstor.dbdrivers.interfaces.SatelliteConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import javax.inject.Provider;

public class SatelliteConnectionData extends BaseTransactionObject implements SatelliteConnection
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Node node;

    private final NetInterface netIf;

    private final TransactionSimpleObject<SatelliteConnectionData, TcpPortNumber> port;

    private final TransactionSimpleObject<SatelliteConnectionData, EncryptionType> encryptionType;

    private final SatelliteConnectionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<SatelliteConnectionData, Boolean> deleted;

    SatelliteConnectionData(
        UUID uuid,
        Node nodeRef,
        NetInterface netIfRef,
        TcpPortNumber portRef,
        EncryptionType encryptionTypeRef,
        SatelliteConnectionDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        ErrorCheck.ctorNotNull(SatelliteConnectionData.class, Node.class, nodeRef);
        ErrorCheck.ctorNotNull(SatelliteConnectionData.class, NetInterface.class, netIfRef);

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        node = nodeRef;
        netIf = netIfRef;
        dbDriver = dbDriverRef;

        port = transObjFactory.createTransactionSimpleObject(
            this,
            portRef,
            dbDriver.getSatelliteConnectionPortDriver()
        );
        encryptionType = transObjFactory.createTransactionSimpleObject(
            this,
            encryptionTypeRef,
            dbDriver.getSatelliteConnectionTypeDriver()
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            node,
            netIf,
            port,
            encryptionType,
            deleted
        );
        activateTransMgr();
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
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
    public TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber newPort)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return port.set(newPort);
    }

    @Override
    public EncryptionType getEncryptionType()
    {
        checkDeleted();
        return encryptionType.get();
    }

    @Override
    public EncryptionType setEncryptionType(AccessContext accCtx, EncryptionType newEncryptionType)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return encryptionType.set(newEncryptionType);
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            ((NodeData) node).removeSatelliteconnection(accCtx, this);
            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted satellite connection");
        }
    }
}
