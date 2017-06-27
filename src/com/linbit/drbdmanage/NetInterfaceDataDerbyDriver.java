package com.linbit.drbdmanage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.dbdrivers.UpdateOnlyDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public class NetInterfaceDataDerbyDriver implements NetInterfaceDataDatabaseDriver
{
    private static final String TBL_NODE_NET = DerbyConstants.TBL_NODE_NET_INTERFACES;

    private static final String NODE_NAME = DerbyConstants.NODE_NAME;
    private static final String NET_NAME = DerbyConstants.NODE_NET_NAME;
    private static final String NET_DSP_NAME = DerbyConstants.NODE_NET_DSP_NAME;
    private static final String INET_ADDRESS = DerbyConstants.INET_ADDRESS;
    private static final String INET_TYPE = DerbyConstants.INET_TRANSPORT_TYPE;

    private static final String NNI_SELECT =
        " SELECT " + NET_DSP_NAME + ", " + INET_ADDRESS + ", " + INET_TYPE +
        " FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";

    private static final String NNI_INSERT =
        " INSERT INTO " + TBL_NODE_NET +
        " VALUES (?, ?, ?, ?, ?)";
    private static final String NNI_DELETE =
        " DELETE FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";
    private static final String NNI_UPDATE_ADR =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_ADDRESS  + " = ?, " +
        "       " + INET_TYPE     + " = ? " +
        " WHERE " + NODE_NAME     + " = ? AND " +
        "       " + NET_NAME      + " = ?";
    private static final String NNI_UPDATE_TYPE =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_TYPE + " = ? " +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";

    private final Node node;
    private final NetInterfaceName netName;

    private final ObjectDatabaseDriver<InetAddress> netIfAddressDriver;
    private final ObjectDatabaseDriver<NetInterfaceType> netIfTypeDriver;

    private final AccessContext dbCtx;

    public NetInterfaceDataDerbyDriver(AccessContext ctx, Node nodeRef, NetInterfaceName nameRef)
    {
        dbCtx = ctx;
        node = nodeRef;
        netName = nameRef;

        netIfAddressDriver = new NodeNetInterfaceAddressDriver();
        netIfTypeDriver = new NodeNetInterfaceTypeDriver();
    }

    @Override
    public NetInterfaceData load(Connection con, AccessContext accCtx, TransactionMgr transMgr)
        throws SQLException, AccessDeniedException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, netName.value);
        ResultSet resultSet = stmt.executeQuery();
        NetInterfaceData netIfData = null;
        if (resultSet.next())
        {
            InetAddress addr;
            String type = resultSet.getString(INET_TYPE);
            try
            {
                addr = InetAddress.getByName(resultSet.getString(INET_ADDRESS));
            }
            catch (UnknownHostException unknownHostExc)
            {
                throw new DrbdSqlRuntimeException(
                    "SQL-based change to NetInterface's host caused an UnknownHostException",
                    unknownHostExc
                );
            }
            netIfData = new NetInterfaceData(
                accCtx,
                node,
                netName,
                addr,
                transMgr,
                NetInterfaceType.byValue(type)
            );
        }
        return netIfData;
    }

    @Override
    public void create(Connection con, NetInterfaceData netInterfaceData) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_INSERT);

        InetAddress inetAddress = getAddress(netInterfaceData);
        NetInterfaceType type = getNetInterfaceType(netInterfaceData);

        stmt.setString(1, node.getName().value);
        stmt.setString(2, netInterfaceData.getName().value);
        stmt.setString(3, netInterfaceData.getName().displayValue);
        stmt.setString(4, inetAddress.getHostAddress());
        stmt.setString(5, type.name());

        stmt.executeUpdate();
        stmt.close();
    }

    public void ensureEntryExists(Connection con, NetInterfaceData netIfData) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, netName.value);
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            boolean equals = true;
            equals &= getAddress(netIfData).getHostAddress().equals(resultSet.getString(INET_ADDRESS));
            equals &= getNetInterfaceType(netIfData).name().equals(resultSet.getString(INET_TYPE));
            if (!equals)
            {
                throw new DrbdSqlRuntimeException("A emporary NetInterfaceData instance is not allowed to override a persisted instance.");
            }
        }
        else
        {
            create(con, netIfData);
        }
    }

    public void delete(Connection con, NetInterface nid) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_DELETE);

        stmt.setString(1, node.getName().value);
        stmt.setString(2, nid.getName().value);

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public ObjectDatabaseDriver<InetAddress> getNetInterfaceAddressDriver()
    {
        return netIfAddressDriver;
    }

    @Override
    public ObjectDatabaseDriver<NetInterfaceType> getNetInterfaceTypeDriver()
    {
        return netIfTypeDriver;
    }

    private InetAddress getAddress(NetInterface value)
    {
        try
        {
            return value.getAddress(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError("NodeDerbyDriver should have a clone of sysCtx", accessDeniedExc);
        }
    }

    private NetInterfaceType getNetInterfaceType(NetInterface value)
    {
        try
        {
            return value.getNetInterfaceType(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError("NodeDerbyDriver should have a clone of sysCtx", accessDeniedExc);
        }
    }

    private class NodeNetInterfaceAddressDriver extends UpdateOnlyDatabaseDriver<InetAddress>
    {
        public NodeNetInterfaceAddressDriver()
        {
            super(TBL_NODE_NET + "." + INET_ADDRESS);
        }

        @Override
        public void update(Connection con, InetAddress inetAddress) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NNI_UPDATE_ADR);

            stmt.setString(1, inetAddress.getHostName());
            stmt.setString(2, node.getName().value);
            stmt.setString(3, netName.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class NodeNetInterfaceTypeDriver extends UpdateOnlyDatabaseDriver<NetInterfaceType>
    {
        public NodeNetInterfaceTypeDriver()
        {
            super(TBL_NODE_NET + "." + INET_TYPE);
        }

        @Override
        public void update(Connection con, NetInterfaceType type) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NNI_UPDATE_TYPE);

            stmt.setString(1, type.name());
            stmt.setString(2, node.getName().value);
            stmt.setString(3, netName.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
