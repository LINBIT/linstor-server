package com.linbit.drbdmanage;

import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.TransactionSimpleObject;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Implementation of a network interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NetInterfaceData extends BaseTransactionObject implements NetInterface
{
    private UUID niUuid;
    private Node niNode;
    private NetInterfaceName niName;

    private ObjectProtection objProt;
    private TransactionSimpleObject<InetAddress> niAddress;
    private TransactionSimpleObject<NetInterfaceType> niType;

    private final NetInterfaceDataDatabaseDriver dbDriver;

    NetInterfaceData(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        InetAddress addr,
        TransactionMgr transMgr,
        NetInterfaceType netType
    )
        throws SQLException, AccessDeniedException
    {
        niUuid = UUID.randomUUID();
        niNode = node;
        niName = name;
        dbDriver = DrbdManage.getNetInterfaceDataDatabaseDriver(node, name);

        niAddress = new TransactionSimpleObject<InetAddress>(
            addr,
            dbDriver.getNetInterfaceAddressDriver()
        );
        niType = new TransactionSimpleObject<NetInterfaceType>(
            netType,
            dbDriver.getNetInterfaceTypeDriver()
        );
        objProt = ObjectProtection.getInstance(accCtx, transMgr, ObjectProtection.buildPath(this), true);

        transObjs = Arrays.<TransactionObject> asList(
            niAddress,
            niType
        );
    }

    public static NetInterfaceData getInstance(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        InetAddress addr,
        TransactionMgr transMgr,
        NetInterfaceType netType,
        boolean createIfNotExists
    )
        throws SQLException, AccessDeniedException
    {
        NetInterfaceData netData = null;
        NetInterfaceDataDatabaseDriver driver = DrbdManage.getNetInterfaceDataDatabaseDriver(node, name);

        if (transMgr != null)
        {
            netData = driver.load(transMgr.dbCon, accCtx, transMgr);
        }

        if (netData == null && createIfNotExists)
        {
            netData = new NetInterfaceData(accCtx, node, name, addr, transMgr, netType);
            if (transMgr != null)
            {
                driver.create(transMgr.dbCon, netData);
            }
        }

        if (netData != null)
        {
            netData.initialized();
        }

        return netData;
    }

    @Override
    public UUID getUuid()
    {
        return niUuid;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public NetInterfaceName getName()
    {
        return niName;
    }

    @Override
    public Node getNode()
    {
        return niNode;
    }

    @Override
    public InetAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return niAddress.get();
    }

    @Override
    public void setAddress(AccessContext accCtx, InetAddress newAddress)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        niAddress.set(newAddress);
    }

    @Override
    public NetInterfaceType getNetInterfaceType(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return niType.get();
    }

    @Override
    public void setNetInterfaceType(AccessContext accCtx, NetInterfaceType type)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        niType.set(type);
    }
}
