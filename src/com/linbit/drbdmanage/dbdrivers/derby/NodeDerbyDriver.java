package com.linbit.drbdmanage.dbdrivers.derby;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.linbit.MapDatabaseDriver;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.NetInterface;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class NodeDerbyDriver implements NodeDatabaseDriver
{
    public static final String TBL_NODE = "NODES";
    public static final String NODE_NAME = "NODE_NAME";
    public static final String NODE_DSP_NAME = "NODE_DSP_NAME";
    public static final String NODE_FLAGS = "NODE_FLAGS";
    public static final String NODE_TYPE = "NODE_TYPE";

    private static final String UPDATE_FLAGS =
        " UPDATE " + TBL_NODE +
        " SET " + NODE_FLAGS + " = ? " +
        " WHERE " + NODE_NAME + " = ?";


    private Connection con;
    private StateFlagsPersistence stateFlagPersistence;
    protected String instanceName;
    private MapDatabaseDriver<ResourceName, Resource> nodeResourceMapDriver;
    private MapDatabaseDriver<NetInterfaceName, NetInterface> nodeNetInterfaceMapDriver;
    private MapDatabaseDriver<StorPoolName, StorPool> nodeStorPoolMapDriver;
    private ObjectDatabaseDriver<NodeType> nodeTypeDriver;

    public NodeDerbyDriver(final String instanceName)
    {
        this.instanceName = instanceName;
        nodeTypeDriver = new NodeTypeDriver();
        stateFlagPersistence = new FlagPersistence();
        nodeResourceMapDriver = new NodeResourceMapDriver();
        nodeNetInterfaceMapDriver = new NodeNetInterfaceMapDriver();
        nodeStorPoolMapDriver = new NodeStorPoolMapDriver();
    }

    @Override
    public StateFlagsPersistence getStateFlagPersistence()
    {
        return stateFlagPersistence;
    }

    @Override
    public ObjectDatabaseDriver<NodeType> getNodeTypeDriver()
    {
        return nodeTypeDriver;
    }


    @Override
    public void setConnection(Connection dbCon)
    {
        con = dbCon;
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

    private class NodeTypeDriver implements ObjectDatabaseDriver<NodeType>
    {

        @Override
        public void setConnection(Connection con)
        {
            // TODO Auto-generated method stub
        }

        @Override
        public void insert(NodeType element) throws SQLException
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void update(NodeType element) throws SQLException
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void delete(NodeType element) throws SQLException
        {
            // TODO Auto-generated method stub

        }

    }

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
            // TODO Auto-generated method stub

        }

        @Override
        public void insert(InetAddress element) throws SQLException
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void update(InetAddress element) throws SQLException
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void delete(InetAddress element) throws SQLException
        {
            // TODO Auto-generated method stub

        }
    }

    private class NodeResourceMapDriver implements MapDatabaseDriver<ResourceName, Resource>
    {

        @Override
        public void setConnection(Connection con)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void insert(ResourceName key, Resource value)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void update(ResourceName key, Resource value)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void delete(ResourceName key)
        {
            // TODO Auto-generated method stub

        }
    }

    private class NodeNetInterfaceMapDriver implements MapDatabaseDriver<NetInterfaceName, NetInterface>
    {

        @Override
        public void setConnection(Connection con)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void insert(NetInterfaceName key, NetInterface value)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void update(NetInterfaceName key, NetInterface value)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void delete(NetInterfaceName key)
        {
            // TODO Auto-generated method stub

        }
    }

    private class NodeStorPoolMapDriver implements MapDatabaseDriver<StorPoolName, StorPool>
    {

        @Override
        public void setConnection(Connection con)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void insert(StorPoolName key, StorPool value)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void update(StorPoolName key, StorPool value)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void delete(StorPoolName key)
        {
            // TODO Auto-generated method stub

        }

    }

    private class FlagPersistence implements StateFlagsPersistence
    {
        @Override
        public void persist(Connection dbConn, long flags) throws SQLException
        {
            if (dbConn != null)
            {
                PreparedStatement stmt = dbConn.prepareStatement(UPDATE_FLAGS);

                stmt.setLong(1, flags);
                stmt.setString(2, instanceName);

                stmt.executeUpdate();
                stmt.close();
            }
            else
            {
                // TODO: log warning that an update has been missed
            }
        }
    }

}
