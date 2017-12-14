package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

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

    private final TransactionSimpleObject<NetInterfaceData, LsIpAddress> niAddress;

    private final NetInterfaceDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    // used by getInstance
    private NetInterfaceData(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        LsIpAddress addr,
        TransactionMgr transMgr
    )
        throws AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            name,
            node,
            addr
        );

        setConnection(transMgr);
    }

    // used by db drivers and tests
    NetInterfaceData(
        UUID uuid,
        AccessContext accCtx,
        NetInterfaceName netName,
        Node node,
        LsIpAddress addr
    )
        throws AccessDeniedException
    {
        niUuid = uuid;
        niNode = node;
        niName = netName;

        dbDriver = LinStor.getNetInterfaceDataDatabaseDriver();

        niAddress = new TransactionSimpleObject<>(
            this,
            addr,
            dbDriver.getNetInterfaceAddressDriver()
        );

        transObjs = Arrays.<TransactionObject> asList(niAddress);
        ((NodeData) node).addNetInterface(accCtx, this);
    }

    public static NetInterfaceData getInstance(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        LsIpAddress addr,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        NetInterfaceData netData = null;
        NetInterfaceDataDatabaseDriver driver = LinStor.getNetInterfaceDataDatabaseDriver();

        netData = driver.load(node, name, false, transMgr);

        if (failIfExists && netData != null)
        {
            throw new LinStorDataAlreadyExistsException("The NetInterface already exists");
        }

        if (netData == null && createIfNotExists)
        {
            netData = new NetInterfaceData(accCtx, node, name, addr, transMgr);
            driver.create(netData, transMgr);
        }
        if (netData != null)
        {
            netData.initialized();
        }

        return netData;
    }

    public static NetInterfaceData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        NetInterfaceDataDatabaseDriver driver = LinStor.getNetInterfaceDataDatabaseDriver();

        NetInterfaceData netData;
        try
        {
            netData = driver.load(node, netName, false, transMgr);
            if (netData == null)
            {
                netData = new NetInterfaceData(
                    uuid,
                    accCtx,
                    netName,
                    node,
                    addr
                );
            }
            netData.initialized();
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
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
    public void setAddress(AccessContext accCtx, LsIpAddress newAddress)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        niAddress.set(newAddress);
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        ((NodeData) niNode).removeNetInterface(accCtx, this);
        dbDriver.delete(this, transMgr);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted NetInterface", null);
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
        return "Node: '" + niNode.getName() + "', "+
               "NetInterfaceName: '" + niName + "'";
    }
}
