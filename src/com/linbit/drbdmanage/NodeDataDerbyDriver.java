package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
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


    private Map<NodeName, Node> nodeCache;
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<NodeData> flagDriver;
    private final SingleColumnDatabaseDriver<NodeData, NodeType> typeDriver;


    public NodeDataDerbyDriver(
        AccessContext privCtx,
        ErrorReporter errorReporterRef,
        Map<NodeName, Node> nodeCache
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        this.nodeCache = nodeCache;

        flagDriver = new NodeFlagPersistence();
        typeDriver = new NodeTypeDriver();
    }

    @Override
    public void create(NodeData node, TransactionMgr transMgr) throws SQLException
    {
        try
        {
            errorReporter.logDebug("Creating Node (NodeName=" + node.getName().value + ")");

            PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_INSERT);
            stmt.setBytes(1, UuidUtils.asByteArray(node.getUuid()));
            stmt.setString(2, node.getName().value);
            stmt.setString(3, node.getName().displayValue);
            stmt.setLong(4, node.getFlags().getFlagsBits(dbCtx));
            stmt.setLong(5, node.getNodeType(dbCtx).getFlagValue());
            stmt.setString(6, ObjectProtection.buildPath(node.getName()));
            stmt.executeUpdate();
            stmt.close();

            cache(node);
            errorReporter.logTrace("Node created (NodeName=" + node.getName().displayValue + ")");
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
    public NodeData load(NodeName nodeName, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException
    {
        try
        {
            String nodeDebug = "(NodeName=" + nodeName.value + ")";
            errorReporter.logDebug("Loading node " + nodeDebug);

            PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_SELECT);
            stmt.setString(1, nodeName.value);
            ResultSet resultSet = stmt.executeQuery();

            NodeData node = cacheGet(nodeName);
            if (node == null)
            {
                if (resultSet.next())
                {
                    try
                    {
                        nodeName = new NodeName(resultSet.getString(NODE_DSP_NAME));
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
                    ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
                    ObjectProtection objProt = objProtDriver.loadObjectProtection(
                        ObjectProtection.buildPath(nodeName),
                        transMgr
                    );

                    node = new NodeData(
                        UuidUtils.asUuid(resultSet.getBytes(NODE_UUID)),
                        objProt,
                        nodeName,
                        Node.NodeType.getByValue(resultSet.getLong(NODE_TYPE)),
                        resultSet.getLong(NODE_FLAGS),
                        serialGen,
                        transMgr
                    );
                    if (cache(node))
                    {
                        errorReporter.logDebug("Restoring netInterfaceData " + nodeDebug);
                        List<NetInterfaceData> netIfaces = NetInterfaceDataDerbyDriver.loadNetInterfaceData(node, transMgr);
                        for (NetInterfaceData netIf : netIfaces)
                        {
                            node.addNetInterface(dbCtx, netIf);
                        }

                        errorReporter.logDebug("Restoring resourceData " + nodeDebug);
                        List<ResourceData> resList = ResourceDataDerbyDriver.loadResourceData(dbCtx, node, serialGen, transMgr);
                        for (ResourceData res : resList)
                        {
                            node.addResource(dbCtx, res);
                        }

                        errorReporter.logDebug("Restoring storPools " + nodeDebug);
                        List<StorPoolData> storPoolList = StorPoolDataDerbyDriver.loadStorPools(node, serialGen, transMgr);
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
                else
                {
                    errorReporter.logWarning(
                        String.format(
                            "The specified node (%s) was not found in the database",
                            nodeName.displayValue
                        )
                    );
                }
            }
            else
            {
                errorReporter.logDebug("Node loaded from cache");
                if (!resultSet.next())
                {
                    errorReporter.reportError(
                        new DrbdSqlRuntimeException(
                            "Cached Node was not found in the database",
                            "The database entry from a node which was loaded from cache is missing",
                            "That could only happen if a user manually deleted that node during runtime",
                            null,
                            null
                        )
                    );
                }
            }

            resultSet.close();
            stmt.close();

            errorReporter.logTrace("Node loaded successfully (NodeName=" + nodeName.displayValue + ")");

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
    public void delete(NodeData node, TransactionMgr transMgr) throws SQLException
    {
        NodeName nodeName = node.getName();
        errorReporter.logDebug("Deleting node (NodeName=" + nodeName.value + ")");

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_DELETE);

        stmt.setString(1, nodeName.value);

        stmt.executeUpdate();
        stmt.close();

        cacheRemove(nodeName);
        errorReporter.logTrace("Node deleted (NodeName=" + nodeName.displayValue + ")");
    }

    private boolean cache(NodeData node)
    {
        boolean ret = false;
        if (node != null)
        {
            NodeName pk = node.getName();
            boolean contains = nodeCache.containsKey(pk);
            if (!contains)
            {
                nodeCache.put(pk, node);
                ret = true;
            }
        }
        return ret;
    }

    private void cacheRemove(NodeName nodeName)
    {
        if (nodeName != null)
        {
            nodeCache.remove(nodeName);
        }
    }

    private NodeData cacheGet(NodeName nodeName)
    {
        return (NodeData) nodeCache.get(nodeName);
    }

    @Override
    public StateFlagsPersistence<NodeData> getStateFlagPersistence()
    {
        return flagDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NodeData, NodeType> getNodeTypeDriver()
    {
        return typeDriver;
    }

    private class NodeFlagPersistence implements StateFlagsPersistence<NodeData>
    {
        @Override
        public void persist(NodeData node, long flags, TransactionMgr transMgr) throws SQLException
        {
            NodeName nodeName = node.getName();
            errorReporter.logDebug("Updating node flags (NodeName=" + nodeName.value + ")");
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_UPDATE_FLAGS);

            stmt.setLong(1, flags);
            stmt.setString(2, nodeName.value);

            stmt.executeUpdate();
            stmt.close();
            errorReporter.logTrace("Node flags updated (NodeName=" + nodeName.displayValue + ")");
        }
    }

    private class NodeTypeDriver implements SingleColumnDatabaseDriver<NodeData, NodeType>
    {
        @Override
        public void update(NodeData parent, NodeType element, TransactionMgr transMgr) throws SQLException
        {
            NodeName nodeName = parent.getName();
            errorReporter.logDebug("Updating node type (NodeName=" + nodeName.value + ")");

            PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_UPDATE_TYPE);

            stmt.setLong(1, element.getFlagValue());
            stmt.setString(2, nodeName.value);

            stmt.executeUpdate();
            stmt.close();

            errorReporter.logDebug("Node type updated (NodeName=" + nodeName.displayValue + ")");
        }
    }
}
