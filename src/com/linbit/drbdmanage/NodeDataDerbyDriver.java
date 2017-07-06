package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import com.linbit.ImplementationError;
import com.linbit.MapDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.InvalidValueException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsConDerbyDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class NodeDataDerbyDriver implements NodeDataDatabaseDriver
{
    private static final String TBL_NODE = DerbyConstants.TBL_NODES;

    private static final String NODE_UUID = DerbyConstants.UUID;
    private static final String NODE_NAME = DerbyConstants.NODE_NAME;
    private static final String NODE_DSP_NAME = DerbyConstants.NODE_DSP_NAME;
    private static final String NODE_FLAGS = DerbyConstants.NODE_FLAGS;
    private static final String NODE_TYPE = DerbyConstants.NODE_TYPE;
    private static final String OBJ_PATH = DerbyConstants.OBJECT_PATH;

    private static final String NODE_SELECT =
        " SELECT " + NODE_UUID + ", " + NODE_DSP_NAME + ", " + NODE_TYPE + ", " + NODE_FLAGS + ", " + OBJ_PATH +
        " FROM " + TBL_NODE +
        " WHERE " + NODE_NAME + " = ?";
    private static final String NODE_INSERT =
        " INSERT INTO " + TBL_NODE +
        " VALUES (?, ?, ?, ?, ?, ?)";
    private static final String NODE_UPDATE_FLAGS =
        " UPDATE " + TBL_NODE +
        " SET "   + NODE_FLAGS + " = ? " +
        " WHERE " + NODE_NAME +  " = ?";
    private static final String NODE_UPDATE_TYPE =
        " UPDATE " + TBL_NODE +
        " SET "   + NODE_TYPE + " = ? " +
        " WHERE " + NODE_NAME + " = ?";
    private static final String NODE_DELETE =
        " DELETE FROM " + TBL_NODE +
        " WHERE " + NODE_NAME + " = ?";


    private static Hashtable<PrimaryKey, NodeData> nodeCache = new Hashtable<>();
    private final AccessContext dbCtx;

    private MapDatabaseDriver<ResourceName, Resource> nodeResourceMapDriver;

    public NodeDataDerbyDriver(AccessContext privCtx)
    {
        dbCtx = privCtx;
        nodeResourceMapDriver = new NodeResourceMapDriver();
    }

    @Override
    public void create(Connection con, NodeData node) throws SQLException
    {
        try
        {
            PreparedStatement stmt = con.prepareStatement(NODE_INSERT);
            stmt.setBytes(1, UuidUtils.asByteArray(node.getUuid()));
            stmt.setString(2, node.getName().value);
            stmt.setString(3, node.getName().displayValue);
            stmt.setLong(4, node.getFlags().getFlagsBits(dbCtx));
            stmt.setLong(5, node.getNodeTypes(dbCtx));
            stmt.setString(6, ObjectProtection.buildPath(node.getName()));
            stmt.executeUpdate();
            stmt.close();

            cache(node);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access NodeFlags and NodeTypes",
                accessDeniedExc
            );
        }
    }

    @Override
    public NodeData load(Connection con, NodeName nodeName, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException
    {
        try
        {
            PreparedStatement stmt = con.prepareStatement(NODE_SELECT);
            stmt.setString(1, nodeName.value);
            ResultSet resultSet = stmt.executeQuery();

            NodeData node = cacheGet(nodeName);
            if (node == null)
            {
                if (resultSet.next())
                {
                    ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                        ObjectProtection.buildPath(nodeName)
                    );
                    ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

                    Set<NodeType> types = new HashSet<>();
                    long typeBits = resultSet.getLong(NODE_TYPE);
                    for (NodeType type : NodeType.values())
                    {
                        if ((typeBits & type.getFlagValue()) == type.getFlagValue())
                        {
                            types.add(type);
                        }
                    }
                    Set<NodeFlag> flags = new HashSet<>();
                    long flagBits = resultSet.getLong(NODE_FLAGS);
                    for (NodeFlag flag : NodeFlag.values())
                    {
                        if ((flagBits & flag.flagValue) == flag.flagValue)
                        {
                            flags.add(flag);
                        }
                    }

                    node = new NodeData(
                        UuidUtils.asUUID(resultSet.getBytes(NODE_UUID)),
                        objProt,
                        nodeName,
                        types,
                        flags,
                        serialGen,
                        transMgr
                    );

                    // restore netInterfaces
                    List<NetInterfaceData> netIfaces = NetInterfaceDataDerbyDriver.loadNetInterfaceData(con, node);
                    for (NetInterfaceData netIf : netIfaces)
                    {
                        node.addNetInterface(dbCtx, netIf);
                    }


                    // restore props
                    PropsConDatabaseDriver propDriver = DrbdManage.getPropConDatabaseDriver(PropsContainer.buildPath(nodeName));
                    Props props = node.getProps(dbCtx);
                    Map<String, String> loadedProps = propDriver.load(con);
                    for (Entry<String, String> entry : loadedProps.entrySet())
                    {
                        try
                        {
                            props.setProp(entry.getKey(), entry.getValue());
                        }
                        catch (InvalidKeyException | InvalidValueException invalidException)
                        {
                            resultSet.close();
                            stmt.close();
                            throw new DrbdSqlRuntimeException(
                                "Invalid property loaded from instance: " + PropsContainer.buildPath(nodeName),
                                invalidException
                            );
                        }
                    }

                    // restore resources
                    List<ResourceData> resList = ResourceDataDerbyDriver.loadResourceData(con, dbCtx, node, serialGen, transMgr);
                    for (ResourceData res : resList)
                    {
                        node.addResource(dbCtx, res);
                    }

                    // restore storPools
                    List<StorPoolData> storPoolList = StorPoolDataDerbyDriver.loadStorPools(con, node, transMgr, serialGen);
                    for (StorPoolData storPool : storPoolList)
                    {
                        node.addStorPool(dbCtx, storPool);
                    }
                    cache(node);
                }
            }
            else
            {
                // we have a cached node
                if (!resultSet.next())
                {
                    // but no entry in the db..

                    // XXX: user deleted db entry during runtime - throw exception?
                    // or just remove the item from the cache + node.removeRes(cachedRes) + warn the user to not do that again otherwise we will format all devices?
                }
            }

            resultSet.close();
            stmt.close();


            return node;
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to fully restore NodeData",
                accessDeniedExc
            );
        }
    }

    @Override
    public void delete(Connection con, NodeData node) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NODE_DELETE);

        stmt.setString(1, node.getName().value);

        stmt.executeUpdate();
        stmt.close();

        // TODO: gh - also delete all its resources, and other sub-objects?
        cacheRemove(node);
    }

    private static void cache(NodeData node)
    {
        if (node != null)
        {
            nodeCache.put(getPk(node), node);
        }
    }

    private static void cacheRemove(NodeData node)
    {
        if (node != null)
        {
            nodeCache.remove(getPk(node));
        }
    }

    private static NodeData cacheGet(NodeName nodeName)
    {
        return nodeCache.get(getPk(nodeName));
    }

    private static PrimaryKey getPk(NodeName nodeName)
    {
        return new PrimaryKey(nodeName.value);
    }

    private static PrimaryKey getPk(NodeData node)
    {
        return new PrimaryKey(node.getName().value);
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static void clearCache()
    {
        nodeCache.clear();
    }

    @Override
    public StateFlagsPersistence getStateFlagPersistence(NodeName nodeName)
    {
        return new NodeFlagPersistence(nodeName);
    }

    @Override
    public StateFlagsPersistence getNodeTypeStateFlagPersistence(NodeName nodeName)
    {
        return new NodeTypePersistence(nodeName);
    }

    @Override
    public MapDatabaseDriver<ResourceName, Resource> getNodeResourceMapDriver(NodeName nodeName)
    {
        return nodeResourceMapDriver;
    }

    @Override
    public MapDatabaseDriver<NetInterfaceName, NetInterface> getNodeNetInterfaceMapDriver(Node node)
    {
        return new NodeNetInterfaceMapDriver(node);
    }

    @Override
    public MapDatabaseDriver<StorPoolName, StorPool> getNodeStorPoolMapDriver(Node node)
    {
        return new NodeStorPoolMapDriver(node);
    };

    @Override
    public PropsConDatabaseDriver getPropsConDriver(NodeName nodeName)
    {
        return new PropsConDerbyDriver(PropsContainer.buildPath(nodeName));
    }

    private class NodeResourceMapDriver implements MapDatabaseDriver<ResourceName, Resource>
    {
        @Override
        public void insert(Connection con, ResourceName key, Resource value) throws SQLException
        {
            ResourceDataDerbyDriver.ensureResExists(con, (ResourceData) value, dbCtx);
        }

        @Override
        public void update(Connection con, ResourceName key, Resource oldValue, Resource value) throws SQLException
        {
            // we only persist the link from Node to Resource
            // changes on the Resource (the value) will trigger Resource's driver to persist those changes
        }

        @Override
        public void delete(Connection con, ResourceName key, Resource value) throws SQLException
        {
            ResourceDataDerbyDriver.deleteRes(con, (ResourceData) value);
        }
    }

    private class NodeNetInterfaceMapDriver implements MapDatabaseDriver<NetInterfaceName, NetInterface>
    {
        private Node node;

        public NodeNetInterfaceMapDriver(Node nodeRef)
        {
            node = nodeRef;
        }

        @Override
        public void insert(Connection con, NetInterfaceName key, NetInterface value) throws SQLException
        {
            NetInterfaceDataDerbyDriver driver = (
                (NetInterfaceDataDerbyDriver)
                    DrbdManage.getNetInterfaceDataDatabaseDriver(node, key)
            );
            driver.ensureEntryExists(con, (NetInterfaceData) value);
        }

        @Override
        public void update(Connection con, NetInterfaceName key, NetInterface oldValue, NetInterface value)
            throws SQLException
        {
            // it should not be possible to call this method

            // in order for this method to get called, our node would have to perform a
            // map.put(NetInterfaceName, NetInterface), which overrides an existing NetInterface.
            // for this to happen, the old and the new NetInterface have to share the same NetInterfaceName
            // however, as the NetInterface should persist itself to the database on create, it should have
            // already caused a primary key constraint violation

            throw new ImplementationError(
                "Prior this call a violation of primary key constraints should have happend",
                null
            );
        }

        @Override
        public void delete(Connection con, NetInterfaceName key, NetInterface value) throws SQLException
        {
            DrbdManage.getNetInterfaceDataDatabaseDriver(node, key).delete(con, (NetInterfaceData) value);
        }
    }

    private class NodeStorPoolMapDriver implements MapDatabaseDriver<StorPoolName, StorPool>
    {
        private NodeData node;

        public NodeStorPoolMapDriver(Node nodeRef)
        {
            node = (NodeData) nodeRef;
        }

        @Override
        public void insert(Connection con, StorPoolName key, StorPool value) throws SQLException
        {
            // normally the StorPoolData should have inserted itself upon its creation.
            // however, if it was a temporary object and is now added to the node, we have to ensure
            // the StorPoolData is or gets persisted
            getDriver(value).ensureEntryExists(con, (StorPoolData) value);
        }

        @Override
        public void update(Connection con, StorPoolName key, StorPool oldValue, StorPool value) throws SQLException
        {
            // it should not be possible to call this method

            // in order for this method to get called, our node would have to perform a
            // map.put(StorPoolName, StorPool), which overrides an existing StorPool.
            // for this to happen, the old and the new StorPool have to share the same StorPoolName
            // however, as the StorPool should persist itself to the database on create, it should have
            // already caused a primary key constraint violation

            throw new ImplementationError(
                "Prior this call a violation of primary key constraints should have happend",
                null
            );
        }

        @Override
        public void delete(Connection con, StorPoolName key, StorPool value) throws SQLException
        {
            getDriver(value).delete(con, key);
        }

        private StorPoolDataDatabaseDriver getDriver(StorPool storPool)
        {
            try
            {
                return DrbdManage.getStorPoolDataDatabaseDriver(node, storPool.getDefinition(dbCtx));
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                throw new ImplementationError(
                    "Database's access context has no permission to get storPoolDefinition",
                    accessDeniedExc
                );
            }
        }
    }

    private class NodeFlagPersistence implements StateFlagsPersistence
    {
        private NodeName nodeName;

        public NodeFlagPersistence(NodeName nodeNameRef)
        {
            nodeName = nodeNameRef;
        }

        @Override
        public void persist(Connection con, long flags) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_UPDATE_FLAGS);

            stmt.setLong(1, flags);
            stmt.setString(2, nodeName.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class NodeTypePersistence implements StateFlagsPersistence
    {
        private NodeName nodeName;

        public NodeTypePersistence(NodeName nodeNameRef)
        {
            nodeName = nodeNameRef;
        }

        @Override
        public void persist(Connection con, long flags) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NODE_UPDATE_TYPE);

            stmt.setInt(1, (int) flags);
            stmt.setString(2, nodeName.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
