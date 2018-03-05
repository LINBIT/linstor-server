package com.linbit.linstor;

import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import javax.inject.Provider;

/**
 * Implementation of a network interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NetInterfaceData extends BaseTransactionObject implements NetInterface
{
    private final UUID niUuid;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Node niNode;
    private final NetInterfaceName niName;

    private final TransactionSimpleObject<NetInterfaceData, LsIpAddress> niAddress;

    private final NetInterfaceDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<NetInterfaceData, Boolean> deleted;

    NetInterfaceData(
        UUID uuid,
        AccessContext accCtx,
        NetInterfaceName netName,
        Node node,
        LsIpAddress addr,
        NetInterfaceDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
        throws AccessDeniedException
    {
        super(transMgrProviderRef);

        niUuid = uuid;
        niNode = node;
        niName = netName;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        niAddress = transObjFactory.createTransactionSimpleObject(
            this,
            addr,
            dbDriver.getNetInterfaceAddressDriver()
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.<TransactionObject>asList(
            niAddress,
            deleted
        );
        ((NodeData) node).addNetInterface(accCtx, this);
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
        return niUuid;
    }

    @Override
    public NetInterfaceName getName()
    {
        checkDeleted();
        return niName;
    }

    @Override
    public Node getNode()
    {
        checkDeleted();
        return niNode;
    }

    @Override
    public LsIpAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return niAddress.get();
    }

    @Override
    public LsIpAddress setAddress(AccessContext accCtx, LsIpAddress newAddress)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return niAddress.set(newAddress);
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            ((NodeData) niNode).removeNetInterface(accCtx, this);
            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted NetInterface");
        }
    }

    @Override
    public NetInterfaceApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        return new NetInterfacePojo(
                getUuid(),
                getName().getDisplayName(),
                getAddress(accCtx).getAddress()
        );
    }

    @Override
    public String toString()
    {
        return "Node: '" + niNode.getName() + "', " +
               "NetInterfaceName: '" + niName + "'";
    }
}
