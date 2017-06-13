package com.linbit.drbdmanage.dbdrivers.derby;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.linbit.ImplementationError;
import com.linbit.MapDatabaseDriver;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.NetInterface;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class NodeDerbyDriver implements NodeDatabaseDriver
{
    public static final String TBL_NODE = "NODES";
    public static final String TBL_NODE_NET = "NODE_NET_INTERFACES";
    public static final String TBL_NODE_RESOURCE = "NODE_RESOURCE";
    public static final String TBL_NODE_STORE_POOL = "NODE_STOR_POOL";

    public static final String NODE_NAME = "NODE_NAME";
    public static final String NODE_DSP_NAME = "NODE_DSP_NAME";
    public static final String NODE_FLAGS = "NODE_FLAGS";
    public static final String NODE_TYPE = "NODE_TYPE";

    public static final String NODE_NET_NODE_NAME = NODE_NAME;
    public static final String NODE_NET_NODE_NET_NAME = "NODE_NET_NAME";
    public static final String NODE_NET_INET_ADDRESS = "INET_ADDRESS";

    public static final String NODE_RESOURCE_NODE_NAME = NODE_NAME;
    public static final String NODE_RESOURCE_RESOURCE_NAME = "RESOURCE_NAME";

    public static final String NODE_STORE_POOL_NODE_NAME = NODE_NAME;
    public static final String NODE_STORE_POOL_POOL_NAME = "POOL_NAME";

    private static final String NODE_UPDATE_FLAGS =
        " UPDATE " + TBL_NODE +
        " SET "   + NODE_FLAGS + " = ? " +
        " WHERE " + NODE_NAME +  " = ?";
    private static final String NODE_UPDATE_TYPE =
        " UPDATE " + TBL_NODE +
        " SET "   + NODE_TYPE + " = ? " +
        " WHERE " + NODE_NAME + " = ?";

    public static final String NODE_NET_INTERFACE_INSERT =
        " INSERT INTO " + TBL_NODE_NET +
        " VALUES (?, ?, ?)";
    public static final String NODE_NET_INTERFACE_UPDATE =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + NODE_NET_INET_ADDRESS  + " = ? " +
        " WHERE " + NODE_NET_NODE_NAME     + " = ? AND " +
        "       " + NODE_NET_NODE_NET_NAME + " = ?";
    public static final String NODE_NET_INTERFACE_DELETE =
        " DELETE FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NET_NODE_NAME     + " = ? AND " +
        "       " + NODE_NET_NODE_NET_NAME + " = ?";

    private static final String NODE_RESOURCE_INSERT =
        " INSERT INTO " + TBL_NODE_RESOURCE +
        " VALUES (?, ?)";
    private static final String NODE_RESOURCE_DELETE =
        " DELETE FROM " + TBL_NODE_RESOURCE +
        " WHERE " + NODE_RESOURCE_NODE_NAME     + " = ? AND " +
        "       " + NODE_RESOURCE_RESOURCE_NAME + " = ?";

    private static final String NODE_STOR_POOL_INSERT =
        " INSERT INTO " + TBL_NODE_STORE_POOL +
        " VALUES (?, ?)";
    private static final String NODE_STORE_POOL_DELETE =
        " DELETE FROM " + TBL_NODE_STORE_POOL +
        " WHERE " + NODE_STORE_POOL_NODE_NAME + " = ? AND " +
        "       " + NODE_STORE_POOL_POOL_NAME + " = ? ";

    private final AccessContext dbCtx;

    private Connection con;
    private StateFlagsPersistence stateFlagPersistence;
    private StateFlagsPersistence nodeTypeDriver;
    protected String nodeName;
    private MapDatabaseDriver<ResourceName, Resource> nodeResourceMapDriver;
    private MapDatabaseDriver<NetInterfaceName, NetInterface> nodeNetInterfaceMapDriver;
    private MapDatabaseDriver<StorPoolName, StorPool> nodeStorPoolMapDriver;

    public NodeDerbyDriver(AccessContext privCtx, final String instanceName)
    {
        this.dbCtx = privCtx;
        this.nodeName = instanceName;
        nodeTypeDriver = new NodeTypePersistence();
        stateFlagPersistence = new NodeFlagPersistence();
        nodeResourceMapDriver = new NodeResourceMapDriver();
        nodeNetInterfaceMapDriver = new NodeNetInterfaceMapDriver();
        nodeStorPoolMapDriver = new NodeStorPoolMapDriver();
    }

    @Override
    public void setConnection(Connection dbCon)
    {
        con = dbCon;
    }

    @Override
    public StateFlagsPersistence getStateFlagPersistence()
    {
        return stateFlagPersistence;
    }

    @Override
    public StateFlagsPersistence getNodeTypeStateFlagPersistence()
    {
        return nodeTypeDriver;
    }

    @Override
    public ObjectDatabaseDriver<InetAddress> getNodeNetInterfaceDriver(NetInterfaceName netInterfaceName)
    {
        return new NodeNetInterfaceDriver(netInterfaceName);
    }

    @Override
    public MapDatabaseDriver<ResourceName, Resource> getNodeResourceMapDriver()
    {
        return nodeResourceMapDriver;
    }

    @Override
    public MapDatabaseDriver<NetInterfaceName, NetInterface> getNodeNetInterfaceMapDriver()
    {
        return nodeNetInterfaceMapDriver;
    }

    @Override
    public MapDatabaseDriver<StorPoolName, StorPool> getNodeStorPoolMapDriver()
    {
        return nodeStorPoolMapDriver;
    };

    private class NodeNetInterfaceDriver implements ObjectDatabaseDriver<InetAddress>
    {
        private String netInterfaceName;

        public NodeNetInterfaceDriver(NetInterfaceName netInterfaceName)
        {
            this.netInterfaceName = netInterfaceName.value;
        }

        @Override
        public void setConnection(Connection con)
        {
        }

        @Override
        public void insert(InetAddress inetAddress) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_NET_INTERFACE_INSERT);

            stmt.setString(1, nodeName);
            stmt.setString(2, netInterfaceName);
            stmt.setString(3, inetAddress.getHostAddress());

            stmt.executeUpdate();
            stmt.close();
        }

        @Override
        public void update(InetAddress inetAddress) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_NET_INTERFACE_UPDATE);

            stmt.setString(1, inetAddress.getHostAddress());
            stmt.setString(2, nodeName);
            stmt.setString(3, netInterfaceName);

            stmt.executeUpdate();
            stmt.close();
        }

        @Override
        public void delete(InetAddress inetAddress) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_NET_INTERFACE_DELETE);

            stmt.setString(1, nodeName);
            stmt.setString(2, netInterfaceName);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class NodeResourceMapDriver implements MapDatabaseDriver<ResourceName, Resource>
    {
        @Override
        public void setConnection(Connection con)
        {
        }

        @Override
        public void insert(ResourceName key, Resource value) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_RESOURCE_INSERT);

            stmt.setString(1, nodeName);
            stmt.setString(2, key.value);

            stmt.executeUpdate();
            stmt.close();
        }

        @Override
        public void update(ResourceName key, Resource value) throws SQLException
        {
            // we only persist the link from Node to Resource
            // changes on the Resource will trigger Resource's driver to persist those changes
        }

        @Override
        public void delete(ResourceName key) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_RESOURCE_DELETE);

            stmt.setString(1, nodeName);
            stmt.setString(2, key.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class NodeNetInterfaceMapDriver implements MapDatabaseDriver<NetInterfaceName, NetInterface>
    {

        @Override
        public void setConnection(Connection con)
        {
        }

        @Override
        public void insert(NetInterfaceName key, NetInterface value) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_NET_INTERFACE_INSERT);

            InetAddress inetAddress = getAddress(value);

            stmt.setString(1, nodeName);
            stmt.setString(2, key.value);
            stmt.setString(3, inetAddress.getHostAddress());

            stmt.executeUpdate();
            stmt.close();
        }

        @Override
        public void update(NetInterfaceName key, NetInterface value) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_NET_INTERFACE_UPDATE);

            InetAddress inetAddress = getAddress(value);

            stmt.setString(1, inetAddress.getHostAddress());
            stmt.setString(2, nodeName);
            stmt.setString(3, key.value);

            stmt.executeUpdate();
            stmt.close();
        }

        @Override
        public void delete(NetInterfaceName key) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_NET_INTERFACE_DELETE);

            stmt.setString(1, nodeName);
            stmt.setString(2, key.value);

            stmt.executeUpdate();
            stmt.close();
        }

        private InetAddress getAddress(NetInterface value)
        {
            try
            {
                return value.getAddress(dbCtx);
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                throw new ImplementationError(
                    "NodeDerbyDriver should have a clone of sysCtx",
                    accessDeniedExc
                );
            }
        }
    }

    private class NodeStorPoolMapDriver implements MapDatabaseDriver<StorPoolName, StorPool>
    {
        @Override
        public void setConnection(Connection con)
        {
        }

        @Override
        public void insert(StorPoolName key, StorPool value) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_STOR_POOL_INSERT);

            stmt.setString(1, nodeName);
            stmt.setString(2, key.value);

            stmt.executeUpdate();
            stmt.close();
        }

        @Override
        public void update(StorPoolName key, StorPool value) throws SQLException
        {
            // we only persist the link from Node to StorePool
            // changes on the StorePool will trigger StorePool's driver to persist those changes
        }

        @Override
        public void delete(StorPoolName key) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_STORE_POOL_DELETE);

            stmt.setString(1, nodeName);
            stmt.setString(2, key.value);

            stmt.executeUpdate();
            stmt.close();
        }

    }

    private class NodeFlagPersistence implements StateFlagsPersistence
    {
        @Override
        public void persist(Connection dbConn, long flags) throws SQLException
        {
            PreparedStatement stmt = dbConn.prepareStatement(NODE_UPDATE_FLAGS);

            stmt.setLong(1, flags);
            stmt.setString(2, nodeName);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class NodeTypePersistence implements StateFlagsPersistence
    {
        @Override
        public void persist(Connection dbConn, long flags) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_UPDATE_TYPE);

            stmt.setString(1, nodeName);
            stmt.setInt(2, (int) flags);

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
