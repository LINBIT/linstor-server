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
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
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

    private static final String NODE_SELECT =
        " SELECT " + NODE_UUID + ", " + NODE_DSP_NAME + ", " + NODE_TYPE + ", " + NODE_FLAGS +
        " FROM " + TBL_NODE +
        " WHERE " + NODE_NAME + " = ?";
    private static final String NODE_SELECT_ALL =
        " SELECT " + NODE_UUID + ", " + NODE_DSP_NAME + ", " + NODE_TYPE + ", " + NODE_FLAGS +
        " FROM " + TBL_NODE;
    private static final String NODE_INSERT =
        " INSERT INTO " + TBL_NODE +
        " VALUES (?, ?, ?, ?, ?)";
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


    private final Map<NodeName, Node> nodeCache;
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<NodeData> flagDriver;
    private final SingleColumnDatabaseDriver<NodeData, NodeType> typeDriver;

    private NetInterfaceDataDerbyDriver netInterfaceDriver;
    private ResourceDataDerbyDriver resourceDataDriver;
    private StorPoolDataDerbyDriver storPoolDriver;
    private NodeConnectionDataDerbyDriver nodeConnectionDefinitionDriver;

    public NodeDataDerbyDriver(
        AccessContext privCtx,
        ErrorReporter errorReporterRef,
        Map<NodeName, Node> nodeCacheRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        nodeCache = nodeCacheRef;

        flagDriver = new NodeFlagPersistence();
        typeDriver = new NodeTypeDriver();

    }

    public void initialize(
        NetInterfaceDataDerbyDriver netInterfaceDriverRef,
        ResourceDataDerbyDriver resourceDriverRef,
        StorPoolDataDerbyDriver storPoolDriverRef,
        NodeConnectionDataDerbyDriver nodeConnectionDefinitionDriverRef
    )
    {
        netInterfaceDriver = netInterfaceDriverRef;
        resourceDataDriver = resourceDriverRef;
        storPoolDriver = storPoolDriverRef;
        nodeConnectionDefinitionDriver = nodeConnectionDefinitionDriverRef;
    }

    @Override
    public void create(NodeData node, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating Node %s", getTraceId(node));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(node.getUuid()));
            stmt.setString(2, node.getName().value);
            stmt.setString(3, node.getName().displayValue);
            stmt.setLong(4, node.getFlags().getFlagsBits(dbCtx));
            stmt.setLong(5, node.getNodeType(dbCtx).getFlagValue());
            stmt.executeUpdate();
            cache(node);

            errorReporter.logDebug("Node created %s", getDebugId(node));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    public void loadAll(TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Loading all Nodes");
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    load(resultSet, transMgr); // we do not care about the return value
                    // the loaded node(s) get cached anyways, and thus the controller gets
                    // the references that way
                }
            }
        }
        errorReporter.logDebug("Loaded %d Nodes", nodeCache.size());
    }

    @Override
    public NodeData load(NodeName nodeName, TransactionMgr transMgr)
        throws SQLException
    {
        NodeData node = null;
        errorReporter.logTrace("Loading node %s", getTraceId(nodeName));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_SELECT))
        {
            stmt.setString(1, nodeName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    node = load(resultSet, transMgr);
                }
                else
                {
                    errorReporter.logWarning(
                        "Node not found in the DB %s",
                        getDebugId(nodeName)
                    );
                }
            }
        }
        return node;
    }

    private NodeData load(ResultSet resultSet, TransactionMgr transMgr)
        throws SQLException, ImplementationError
    {
        NodeData node;
        NodeName nodeName = null;
        try
        {
            nodeName = new NodeName(resultSet.getString(NODE_DSP_NAME));
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new DrbdSqlRuntimeException(
                "The display name of a stored NodeName could not be restored",
                invalidNameExc
            );
        }

        node = cacheGet(nodeName);
        if (node == null)
        {
            ObjectProtection objProt = getObjectProtection(nodeName, transMgr);

            node = new NodeData(
                UuidUtils.asUuid(resultSet.getBytes(NODE_UUID)),
                objProt,
                nodeName,
                Node.NodeType.getByValue(resultSet.getLong(NODE_TYPE)),
                resultSet.getLong(NODE_FLAGS),
                transMgr
            );

            errorReporter.logTrace("Node instance created %s", getTraceId(node));

            // (-> == loads)
            // node -> resource
            // resource -> resourceDefinition
            // resourceDefinition -> resource (other than before)
            // resource -> node (containing "our" node, but also other nodes)

            // to break this infinite loop we (re-)check our nodesMaps (cache)
            if (cache(node))
            {
                try
                {
                    List<NetInterfaceData> netIfaces =
                        netInterfaceDriver.loadNetInterfaceData(node, transMgr);
                    for (NetInterfaceData netIf : netIfaces)
                    {
                        node.addNetInterface(dbCtx, netIf);
                    }
                    errorReporter.logTrace(
                        "Node's NetInterfaces restored %s Count: %d",
                        getTraceId(node),
                        netIfaces.size()
                    );

                    List<ResourceData> resList = resourceDataDriver.loadResourceData(dbCtx, node, transMgr);
                    for (ResourceData res : resList)
                    {
                        node.addResource(dbCtx, res);
                    }
                    errorReporter.logTrace(
                        "Node's Resources restored %s Count: %d",
                        getTraceId(node),
                        resList.size()
                    );

                    List<StorPoolData> storPoolList = storPoolDriver.loadStorPools(node, transMgr);
                    for (StorPoolData storPool : storPoolList)
                    {
                        node.addStorPool(dbCtx, storPool);
                    }
                    errorReporter.logTrace(
                        "Node's StorPools restored %s Count: %d",
                        getTraceId(node),
                        storPoolList.size()
                    );

                    List<NodeConnectionData> nodeConDfnList =
                        nodeConnectionDefinitionDriver.loadAllByNode(node, transMgr);
                    for (NodeConnectionData nodeConDfn : nodeConDfnList)
                    {
                        node.setNodeConnection(dbCtx, nodeConDfn);
                    }
                    errorReporter.logTrace(
                        "Node's ConnectionDefinitions restored %s Count: %d",
                        getTraceId(node),
                        nodeConDfnList.size()
                    );

                    errorReporter.logDebug("Node loaded from DB %s", getDebugId(node));
                }
                catch (AccessDeniedException accessDeniedExc)
                {
                    DerbyDriver.handleAccessDeniedException(accessDeniedExc);
                }
            }
            else
            {
                node = cacheGet(nodeName);
                errorReporter.logDebug("Node loaded from Cache %s", getDebugId(node));
            }
        }
        else
        {
            errorReporter.logDebug("Node loaded from cache %s", getDebugId(node));
        }
        return node;
    }

    private ObjectProtection getObjectProtection(NodeName nodeName, TransactionMgr transMgr) throws SQLException
    {
        ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(nodeName),
            transMgr
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "Node's DB entry exists, but is missing an entry in ObjProt table! " + getTraceId(nodeName),
                null
            );
        }
        return objProt;
    }

    @Override
    public void delete(NodeData node, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting node %s", getTraceId(node));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_DELETE))
        {
            stmt.setString(1, node.getName().value);

            stmt.executeUpdate();
        }
        cacheRemove(node.getName());
        errorReporter.logDebug("Node deleted %s", getDebugId(node));
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

    private String getTraceId(NodeData node)
    {
        return getId(node.getName().value);
    }

    private String getDebugId(NodeData node)
    {
        return getId(node.getName().displayValue);
    }

    private String getTraceId(NodeName nodeName)
    {
        return getId(nodeName.value);
    }

    private String getDebugId(NodeName nodeName)
    {
        return getId(nodeName.displayValue);
    }

    private String getId(String nodeName)
    {
        return "(NodeName=" + nodeName + ")";
    }

    private class NodeFlagPersistence implements StateFlagsPersistence<NodeData>
    {
        @Override
        public void persist(NodeData node, long flags, TransactionMgr transMgr) throws SQLException
        {
            try
            {
                errorReporter.logTrace("Updating Node's flags from [%s] to [%s] %s",
                    Long.toBinaryString(node.getFlags().getFlagsBits(dbCtx)),
                    Long.toBinaryString(flags),
                    getTraceId(node)
                );
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_UPDATE_FLAGS))
                {
                    stmt.setLong(1, flags);
                    stmt.setString(2, node.getName().value);

                    stmt.executeUpdate();
                }
                errorReporter.logDebug("Node's flags updated from [%s] to [%s] %s",
                    Long.toBinaryString(node.getFlags().getFlagsBits(dbCtx)),
                    Long.toBinaryString(flags),
                    getDebugId(node)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class NodeTypeDriver implements SingleColumnDatabaseDriver<NodeData, NodeType>
    {
        @Override
        public void update(NodeData parent, NodeType element, TransactionMgr transMgr) throws SQLException
        {
            try
            {
                errorReporter.logTrace("Updating Node's NodeType from [%s] to [%s] %s",
                    parent.getNodeType(dbCtx).name(),
                    element.name(),
                    getTraceId(parent)
                );
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_UPDATE_TYPE))
                {
                    stmt.setLong(1, element.getFlagValue());
                    stmt.setString(2, parent.getName().value);

                    stmt.executeUpdate();
                }
                errorReporter.logDebug("Node's NodeType updated from [%s] to [%s] %s",
                    parent.getNodeType(dbCtx).name(),
                    element.name(),
                    getDebugId(parent)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }
}
