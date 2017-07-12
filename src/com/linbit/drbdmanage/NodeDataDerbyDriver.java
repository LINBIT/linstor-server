package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
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


    private static final Hashtable<PrimaryKey, NodeData> nodeCache = new Hashtable<>();
    private final AccessContext dbCtx;

    private final StateFlagsPersistence flagDriver;
    private final StateFlagsPersistence typeFlagDriver;
    private final PropsConDatabaseDriver propsDriver;

    private NodeName nodeName;
    private boolean nodeNameLoaded = false;

    public NodeDataDerbyDriver(AccessContext privCtx, NodeName nodeNameRef)
    {
        dbCtx = privCtx;
        nodeName = nodeNameRef;

        flagDriver = new NodeFlagPersistence();
        typeFlagDriver = new NodeTypePersistence();
        propsDriver = new PropsConDerbyDriver(PropsContainer.buildPath(nodeNameRef));
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
    public NodeData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr) throws SQLException
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
                    if (!nodeNameLoaded)
                    {
                        try
                        {
                            nodeName = new NodeName(resultSet.getString(NODE_DSP_NAME));
                            nodeNameLoaded = true;
                        }
                        catch (InvalidNameException invalidNameExc)
                        {
                            resultSet.close();
                            stmt.close();
                            throw new ImplementationError(
                                "The display name of a valid NodeName could not be restored",
                                invalidNameExc
                            );
                        }
                    }
                    ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                        ObjectProtection.buildPath(nodeName)
                    );
                    ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

                    node = new NodeData(
                        UuidUtils.asUUID(resultSet.getBytes(NODE_UUID)),
                        objProt,
                        nodeName,
                        resultSet.getLong(NODE_TYPE),
                        resultSet.getLong(NODE_FLAGS),
                        serialGen,
                        transMgr
                    );
                    if (cache(node))
                    {

                        // restore netInterfaces
                        List<NetInterfaceData> netIfaces = NetInterfaceDataDerbyDriver.loadNetInterfaceData(con, node);
                        for (NetInterfaceData netIf : netIfaces)
                        {
                            node.addNetInterface(dbCtx, netIf);
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
                    }
                    else
                    {
                        node = cacheGet(nodeName);
                    }
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

    private synchronized static boolean cache(NodeData node)
    {
        boolean ret = false;
        if (node != null)
        {
            PrimaryKey pk = getPk(node);
            boolean contains = nodeCache.containsKey(pk);
            if (!contains)
            {
                nodeCache.put(pk, node);
                ret = true;
            }
        }
        return ret;
    }

    private synchronized static void cacheRemove(NodeData node)
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
     * this method should only be called by tests or if you want a full-resync with the database
     */
    static synchronized void clearCache()
    {
        nodeCache.clear();
    }

    @Override
    public StateFlagsPersistence getStateFlagPersistence()
    {
        return flagDriver;
    }

    @Override
    public StateFlagsPersistence getNodeTypeStateFlagPersistence()
    {
        return typeFlagDriver;
    }

    @Override
    public PropsConDatabaseDriver getPropsConDriver()
    {
        return propsDriver;
    }

    private class NodeFlagPersistence implements StateFlagsPersistence
    {
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
