package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

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
    private final TransactionSimpleObject<NetInterfaceData, TcpPortNumber> niStltConnPort;
    private final TransactionSimpleObject<NetInterfaceData, EncryptionType> niStltConnEncrType;

    private final NetInterfaceDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<NetInterfaceData, Boolean> deleted;

    NetInterfaceData(
        UUID uuid,
        NetInterfaceName netName,
        Node node,
        LsIpAddress addr,
        TcpPortNumber stltConnPortRef,
        EncryptionType stltConnEncrTypeRef,
        NetInterfaceDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
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
        niStltConnPort = transObjFactory.createTransactionSimpleObject(
            this,
            stltConnPortRef,
            dbDriver.getStltConnPortDriver()
        );
        niStltConnEncrType = transObjFactory.createTransactionSimpleObject(
            this,
            stltConnEncrTypeRef,
            dbDriver.getStltConnEncrTypeDriver()
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.<TransactionObject>asList(
            niAddress,
            deleted
        );
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
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return niAddress.set(newAddress);
    }

    @Override
    public boolean isUsableAsStltConn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return niStltConnEncrType.get() != null && niStltConnPort.get() != null;
    }

    @Override
    public boolean setStltConn(AccessContext accCtx, TcpPortNumber port, EncryptionType encrType)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        TcpPortNumber oldPort = niStltConnPort.set(port);
        EncryptionType oldEncrType = niStltConnEncrType.set(encrType);

        return !Objects.equals(oldPort, port) || !Objects.equals(oldEncrType, encrType);
    }

    @Override
    public TcpPortNumber getStltConnPort(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return niStltConnPort.get();
    }

    @Override
    public EncryptionType getStltConnEncryptionType(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return niStltConnEncrType.get();
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
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
        Integer port = null;
        String encrType = null;

        if (niStltConnPort.get() != null && niStltConnEncrType.get() != null)
        {
            port = niStltConnPort.get().value;
            encrType = niStltConnEncrType.get().name();
        }

        return new NetInterfacePojo(
            getUuid(),
            getName().getDisplayName(),
            getAddress(accCtx).getAddress(),
            port,
            encrType
        );
    }

    @Override
    public String toString()
    {
        return "Node: '" + niNode.getName() + "', " +
               "NetInterfaceName: '" + niName + "'";
    }
}
