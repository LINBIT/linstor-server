package com.linbit.drbdmanage;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.TransactionSimpleObject;
import com.linbit.drbdmanage.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
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
    private final UUID niUuid;
    private final Node niNode;
    private final NetInterfaceName niName;

    private final ObjectProtection objProt;
    private final TransactionSimpleObject<DmIpAddress> niAddress;
    private final TransactionSimpleObject<NetInterfaceType> niType;

    private final NetInterfaceDataDatabaseDriver dbDriver;
    private boolean deleted = false;

    // used by getInstance
    private NetInterfaceData(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        DmIpAddress addr,
        TransactionMgr transMgr,
        NetInterfaceType netType
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                transMgr,
                ObjectProtection.buildPath(
                    node.getName(),
                    name
                ),
                true
            ),
            name,
            node,
            addr,
            netType
        );

        if (transMgr != null)
        {
        	setConnection(transMgr);
        }
    }

    // used by db drivers and tests
    NetInterfaceData(
        UUID uuid,
        ObjectProtection objectProtection,
        NetInterfaceName netName,
        Node node,
        DmIpAddress addr,
        NetInterfaceType netType
    )
    {
        niUuid = uuid;
        niNode = node;
        niName = netName;
        objProt = objectProtection;

        dbDriver = DrbdManage.getNetInterfaceDataDatabaseDriver(node, netName);

        niAddress = new TransactionSimpleObject<>(
            addr,
            dbDriver.getNetInterfaceAddressDriver()
        );
        niType = new TransactionSimpleObject<>(
            netType,
            dbDriver.getNetInterfaceTypeDriver()
        );

        transObjs = Arrays.<TransactionObject> asList(
            niAddress,
            niType,
            objProt
        );
    }

    public static NetInterfaceData getInstance(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        DmIpAddress addr,
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
            netData = driver.load(transMgr.dbCon);
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
            // TODO: gh - maybe insert an instanceof check here?
            ((NodeData) node).addNetInterface(accCtx, netData);

            netData.initialized();
        }

        return netData;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return niUuid;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
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
    public DmIpAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return niAddress.get();
    }

    @Override
    public void setAddress(AccessContext accCtx, DmIpAddress newAddress)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        niAddress.set(newAddress);
    }

    @Override
    public NetInterfaceType getNetInterfaceType(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return niType.get();
    }

    @Override
    public void setNetInterfaceType(AccessContext accCtx, NetInterfaceType type)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        niType.set(type);
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);

        dbDriver.delete(dbCon);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted connectionDefinition", null);
        }
    }
}
