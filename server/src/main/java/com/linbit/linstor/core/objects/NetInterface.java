package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
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
public class NetInterface extends AbsCoreObj<NetInterface> implements ProtectedObject
{
    private final Node niNode;
    private final NetInterfaceName niName;

    private final TransactionSimpleObject<NetInterface, LsIpAddress> niAddress;
    private final TransactionSimpleObject<NetInterface, @Nullable TcpPortNumber> niStltConnPort;
    private final TransactionSimpleObject<NetInterface, @Nullable EncryptionType> niStltConnEncrType;

    private final NetInterfaceDatabaseDriver dbDriver;
    private final Key niKey;

    NetInterface(
        UUID uuid,
        NetInterfaceName netName,
        Node node,
        LsIpAddress addr,
        @Nullable TcpPortNumber stltConnPortRef,
        @Nullable EncryptionType stltConnEncrTypeRef,
        NetInterfaceDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(uuid, transObjFactory, transMgrProviderRef);

        niNode = node;
        niName = netName;
        dbDriver = dbDriverRef;

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

        transObjs = Arrays.<TransactionObject>asList(
            niAddress,
            deleted
        );
        niKey = new Key(this);
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

    public Key getKey()
    {
        // no check deleted
        return niKey;
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

    public @Nullable TcpPortNumber getStltConnPort(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return niStltConnPort.get();
    }

    public @Nullable EncryptionType getStltConnEncryptionType(AccessContext accCtx)
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

            niNode.removeNetInterface(accCtx, this);
            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public NetInterfaceApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
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
    public String toStringImpl()
    {
        return "Node: '" + niKey.nodeName + "', " +
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

        public static String nameOfNullable(EncryptionType type)
        {
            return type == null ? null : type.name();
        }
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return niNode.getObjProt();
    }

    /**
     * Identifies a NetInterface.
     */
    public static class Key implements Comparable<Key>
    {
        private final NetInterfaceName netIfName;

        private final NodeName nodeName;

        public Key(NetInterface netIf)
        {
            this(netIf.niName, netIf.niNode.getName());
        }

        public Key(NetInterfaceName netIfNameRef, NodeName nodeNameRef)
        {
            netIfName = netIfNameRef;
            nodeName = nodeNameRef;
        }

        public NetInterfaceName getNetInterfaceName()
        {
            return netIfName;
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(netIfName, nodeName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof Key))
            {
                return false;
            }
            Key other = (Key) obj;
            return Objects.equals(netIfName, other.netIfName) && Objects.equals(nodeName, other.nodeName);
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = netIfName.compareTo(other.netIfName);
            }
            return eq;
        }
    }
}
