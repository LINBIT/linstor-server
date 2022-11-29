package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Implementation of a network interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NetInterface extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<NetInterface>
{
    private final UUID niUuid;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Node niNode;
    private final NetInterfaceName niName;

    private final TransactionSimpleObject<NetInterface, LsIpAddress> niAddress;
    private final TransactionSimpleObject<NetInterface, TcpPortNumber> niStltConnPort;
    private final TransactionSimpleObject<NetInterface, EncryptionType> niStltConnEncrType;

    private final NetInterfaceDatabaseDriver dbDriver;

    private final TransactionSimpleObject<NetInterface, Boolean> deleted;

    NetInterface(
        UUID uuid,
        NetInterfaceName netName,
        Node node,
        LsIpAddress addr,
        TcpPortNumber stltConnPortRef,
        EncryptionType stltConnEncrTypeRef,
        NetInterfaceDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
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

    public UUID getUuid()
    {
        checkDeleted();
        return niUuid;
    }

    public NetInterfaceName getName()
    {
        checkDeleted();
        return niName;
    }

    public Node getNode()
    {
        checkDeleted();
        return niNode;
    }

    public LsIpAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return niAddress.get();
    }

    public LsIpAddress setAddress(AccessContext accCtx, LsIpAddress newAddress)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return niAddress.set(newAddress);
    }

    public boolean isUsableAsStltConn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return niStltConnEncrType.get() != null && niStltConnPort.get() != null;
    }

    public boolean setStltConn(AccessContext accCtx, TcpPortNumber port, EncryptionType encrType)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        TcpPortNumber oldPort = niStltConnPort.set(port);
        EncryptionType oldEncrType = niStltConnEncrType.set(encrType);

        return !Objects.equals(oldPort, port) || !Objects.equals(oldEncrType, encrType);
    }

    public TcpPortNumber getStltConnPort(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return niStltConnPort.get();
    }

    public EncryptionType getStltConnEncryptionType(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return niStltConnEncrType.get();
    }

    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            niNode.removeNetInterface(accCtx, this);
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
    public int compareTo(NetInterface oRef)
    {
        int cmp = niNode.compareTo(oRef.getNode());
        if (cmp == 0)
        {
            cmp = niName.compareTo(oRef.getName());
        }
        return cmp;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(niName, niNode);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof NetInterface)
        {
            NetInterface other = (NetInterface) obj;
            other.checkDeleted();
            ret = Objects.equals(niName, other.niName) && Objects.equals(niNode, other.niNode);
        }
        return ret;
    }

    @Override
    public String toString()
    {
        return "Node: '" + niNode.getName() + "', " +
               "NetInterfaceName: '" + niName + "'";
    }

    public enum EncryptionType
    {
        SSL, PLAIN;

        public static EncryptionType valueOfIgnoreCase(String string)
            throws IllegalArgumentException
        {
            return valueOf(string.toUpperCase());
        }
    }
}
