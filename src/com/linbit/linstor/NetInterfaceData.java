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
    private final TransactionSimpleObject<NetInterfaceData, Integer> niPort;
    private final TransactionSimpleObject<NetInterfaceData, NetInterfaceType> niType;

    private final NetInterfaceDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    // used by getInstance
    private NetInterfaceData(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        LsIpAddress addr,
        int port,
        NetInterfaceType netType,
        TransactionMgr transMgr
    )
        throws AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            name,
            node,
            addr,
            port,
            netType
        );

        setConnection(transMgr);
    }

    // used by db drivers and tests
    NetInterfaceData(
        UUID uuid,
        AccessContext accCtx,
        NetInterfaceName netName,
        Node node,
        LsIpAddress addr,
        int port,
        NetInterfaceType netType
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
        niPort = new TransactionSimpleObject<NetInterfaceData, Integer>(
            this,
            port,
            dbDriver.getNetInterfacePortDriver()
        );
        niType = new TransactionSimpleObject<>(
            this,
            netType,
            dbDriver.getNetInterfaceTypeDriver()
        );

        transObjs = Arrays.<TransactionObject> asList(
            niAddress,
            niType
        );
        ((NodeData) node).addNetInterface(accCtx, this);
    }

    public static NetInterfaceData getInstance(
        AccessContext accCtx,
        Node node,
        NetInterfaceName name,
        LsIpAddress addr,
        int port,
        NetInterfaceType netType,
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
            netData = new NetInterfaceData(accCtx, node, name, addr, port, netType, transMgr);
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
        int port,
        NetInterfaceType netType,
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
                    addr,
                    port,
                    netType
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
    public NetInterfaceType getNetInterfaceType(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return niType.get();
    }

    @Override
    public void setNetInterfaceType(AccessContext accCtx, NetInterfaceType type)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        niType.set(type);
    }

    @Override
    public int getNetInterfacePort(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return niPort.get();
    }

    @Override
    public void setNetInterfacePort(AccessContext accCtx, int port)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        niNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        niPort.set(port);
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

    public NetInterfaceApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        return new NetInterfacePojo(
                getUuid(),
                getName().getDisplayName(),
                getAddress(accCtx).getAddress(),
                getNetInterfaceType(accCtx).toString(),
                getNetInterfacePort(accCtx));
    }

    @Override
    public String toString()
    {
        return "Node: '" + niNode.getName() + "', "+
               "NetInterfaceName: '" + niName + "'";
    }
}
