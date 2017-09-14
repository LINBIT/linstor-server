package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.utils.UuidUtils;

public class NetInterfaceDataDerbyDriver implements NetInterfaceDataDatabaseDriver
{
    private static final String TBL_NODE_NET = DerbyConstants.TBL_NODE_NET_INTERFACES;

    private static final String NODE_NAME = DerbyConstants.NODE_NAME;

    private static final String NET_UUID = DerbyConstants.UUID;
    private static final String NET_NAME = DerbyConstants.NODE_NET_NAME;
    private static final String NET_DSP_NAME = DerbyConstants.NODE_NET_DSP_NAME;
    private static final String INET_ADDRESS = DerbyConstants.INET_ADDRESS;
    private static final String INET_TYPE = DerbyConstants.INET_TRANSPORT_TYPE;

    private static final String NNI_SELECT_BY_NODE =
        " SELECT "  + NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
                      INET_ADDRESS + ", " + INET_TYPE +
        " FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ?";
    private static final String NNI_SELECT_BY_NODE_AND_NET =
        " SELECT "  + NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
                      INET_ADDRESS + ", " + INET_TYPE +
        " FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
                    NET_NAME  + " = ?";

    private static final String NNI_INSERT =
        " INSERT INTO " + TBL_NODE_NET +
        " VALUES (?, ?, ?, ?, ?, ?)";
    private static final String NNI_DELETE =
        " DELETE FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";
    private static final String NNI_UPDATE_ADR =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_ADDRESS  + " = ? " +
        " WHERE " + NODE_NAME     + " = ? AND " +
        "       " + NET_NAME      + " = ?";
    private static final String NNI_UPDATE_TYPE =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_TYPE + " = ? " +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";

    private final SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress> netIfAddressDriver;
    private final SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType> netIfTypeDriver;

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    public NetInterfaceDataDerbyDriver(
        AccessContext ctx,
        ErrorReporter errorReporterRef
    )
    {
        dbCtx = ctx;
        errorReporter = errorReporterRef;

        netIfAddressDriver = new NodeNetInterfaceAddressDriver();
        netIfTypeDriver = new NodeNetInterfaceTypeDriver();
    }

    @Override
    public NetInterfaceData load(Node node, NetInterfaceName niName, TransactionMgr transMgr)
        throws SQLException
    {
        NodeName nodeName = node.getName();

        errorReporter.logDebug(
            "Loading netInterfaceData (Node=%s, SrcNode=%s, DstNode=%s)",
            nodeName.value,
            niName.value
        );


        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NNI_SELECT_BY_NODE_AND_NET);
        stmt.setString(1, nodeName.value);
        stmt.setString(2, niName.value);
        ResultSet resultSet = stmt.executeQuery();
        NetInterfaceData netIfData = cacheGet(node, niName);
        if (netIfData == null)
        {
            if (resultSet.next())
            {
                try
                {
                    niName = new NetInterfaceName(resultSet.getString(NET_DSP_NAME));
                }
                catch (InvalidNameException invalidNameExc)
                {
                    resultSet.close();
                    stmt.close();
                    throw new ImplementationError(
                        "The display name of a valid NetInterfaceName could not be restored",
                        invalidNameExc
                    );
                }

                netIfData = restoreInstance(node, niName, resultSet, transMgr);
            }
        }
        else
        {
            if (!resultSet.next())
            {
                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + detach item from parent (if needed) + warn the user?
            }
        }
        resultSet.close();
        stmt.close();

        return netIfData;
    }

    @Override
    public void create(NetInterfaceData netInterfaceData, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NNI_INSERT);

        DmIpAddress inetAddress = getAddress(netInterfaceData);
        NetInterfaceType type = getNetInterfaceType(netInterfaceData);

        stmt.setBytes(1, UuidUtils.asByteArray(netInterfaceData.getUuid()));
        stmt.setString(2, netInterfaceData.getNode().getName().value);
        stmt.setString(3, netInterfaceData.getName().value);
        stmt.setString(4, netInterfaceData.getName().displayValue);
        stmt.setString(5, inetAddress.getAddress());
        stmt.setString(6, type.name());

        stmt.executeUpdate();
        stmt.close();
    }

    public void ensureEntryExists(NetInterfaceData netIfData, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NNI_SELECT_BY_NODE_AND_NET);
        stmt.setString(1, netIfData.getNode().getName().value);
        stmt.setString(2, netIfData.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        if (!resultSet.next())
        {
            create(netIfData, transMgr);
        }
        resultSet.close();
        stmt.close();
    }

    @Override
    public void delete(NetInterfaceData netInterfaceData, TransactionMgr transMgr) throws SQLException
    {
        Node node = netInterfaceData.getNode();
        NetInterfaceName netName = netInterfaceData.getName();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NNI_DELETE);

        stmt.setString(1, node.getName().value);
        stmt.setString(2, netName.value);

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress> getNetInterfaceAddressDriver()
    {
        return netIfAddressDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType> getNetInterfaceTypeDriver()
    {
        return netIfTypeDriver;
    }


    public List<NetInterfaceData> loadNetInterfaceData(Node node, TransactionMgr transMgr)
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NNI_SELECT_BY_NODE);
        stmt.setString(1, node.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<NetInterfaceData> netIfDataList = new ArrayList<>();
        while (resultSet.next())
        {
            NetInterfaceName netName;
            try
            {
                netName = new NetInterfaceName(resultSet.getString(NET_DSP_NAME));
            }
            catch (InvalidNameException e)
            {
                resultSet.close();
                stmt.close();
                throw new DrbdSqlRuntimeException("NetInterface contains illegal displayName");
            }
            netIfDataList.add(restoreInstance(node, netName, resultSet, transMgr));
        }

        resultSet.close();
        stmt.close();

        return netIfDataList;
    }

    private DmIpAddress getAddress(NetInterface value)
    {
        DmIpAddress ip = null;
        try
        {
            ip = value.getAddress(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
        return ip;
    }

    private NetInterfaceType getNetInterfaceType(NetInterface value)
    {
        NetInterfaceType type = null;
        try
        {
            type = value.getNetInterfaceType(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
        return type;
    }

    private NetInterfaceData restoreInstance(
        Node node,
        NetInterfaceName netName,
        ResultSet resultSet,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        NetInterfaceData ret = cacheGet(node, netName);
        if (ret == null)
        {
            UUID uuid = UuidUtils.asUuid(resultSet.getBytes(NET_UUID));
            DmIpAddress addr;
            String type = resultSet.getString(INET_TYPE);
            try
            {
                addr = new DmIpAddress(resultSet.getString(INET_ADDRESS));
            }
            catch (InvalidIpAddressException invalidIpAddressExc)
            {
                throw new DrbdSqlRuntimeException(
                    "SQL-based change to NetInterface's host caused an UnknownHostException",
                    invalidIpAddressExc
                );
            }
            ObjectProtectionDatabaseDriver opDriver = DrbdManage.getObjectProtectionDatabaseDriver();
            ObjectProtection objProt = opDriver.loadObjectProtection(
                ObjectProtection.buildPath(
                    node.getName(),
                    netName
                ),
                transMgr
            );
            ret = new NetInterfaceData(
                uuid,
                objProt,
                netName,
                node,
                addr,
                NetInterfaceType.byValue(type)
            );
        }

        return ret;
    }

    private NetInterfaceData cacheGet(Node node, NetInterfaceName netName)
    {
        NetInterfaceData ret = null;
        try
        {
            ret = (NetInterfaceData) node.getNetInterface(dbCtx, netName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private class NodeNetInterfaceAddressDriver implements SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress>
    {
        @Override
        public void update(NetInterfaceData parent, DmIpAddress inetAddress, TransactionMgr transMgr) throws SQLException
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(NNI_UPDATE_ADR);

            stmt.setString(1, inetAddress.getAddress());
            stmt.setString(2, parent.getNode().getName().value);
            stmt.setString(3, parent.getName().value);

            stmt.executeUpdate();
            stmt.close();

        }
    }

    private class NodeNetInterfaceTypeDriver implements SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType>
    {
        @Override
        public void update(NetInterfaceData parent, NetInterfaceType type, TransactionMgr transMgr)
            throws SQLException
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(NNI_UPDATE_TYPE);

            stmt.setString(1, type.name());
            stmt.setString(2, parent.getNode().getName().value);
            stmt.setString(3, parent.getName().value);

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
