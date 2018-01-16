package com.linbit.linstor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.StringUtils;
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
        " (" +
            NODE_UUID + ", " + NODE_NAME + ", " + NODE_DSP_NAME + ", " +
            NODE_FLAGS + ", " + NODE_TYPE +
        ") VALUES (?, ?, ?, ?, ?)";
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
    private boolean cacheCleared = false;

    private final Map<NodeName, Node> nodesMap;
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<NodeData> flagDriver;
    private final SingleColumnDatabaseDriver<NodeData, NodeType> typeDriver;

    private NetInterfaceDataDerbyDriver netInterfaceDriver;
    private SatelliteConnectionDataDerbyDriver satelliteConnectionDriver;
    private ResourceDataDerbyDriver resourceDataDriver;
    private StorPoolDataDerbyDriver storPoolDriver;
    private NodeConnectionDataDerbyDriver nodeConnectionDriver;

    public NodeDataDerbyDriver(
        AccessContext privCtx,
        ErrorReporter errorReporterRef,
        Map<NodeName, Node> nodesMapRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        nodeCache = new HashMap<>();

        nodesMap = nodesMapRef;

        flagDriver = new NodeFlagPersistence();
        typeDriver = new NodeTypeDriver();

    }

    public void initialize(
        NetInterfaceDataDerbyDriver netInterfaceDriverRef,
        SatelliteConnectionDataDerbyDriver satelliteConnectionDriverRef,
        ResourceDataDerbyDriver resourceDriverRef,
        StorPoolDataDerbyDriver storPoolDriverRef,
        NodeConnectionDataDerbyDriver nodeConnectionDriverRef
    )
    {
        netInterfaceDriver = netInterfaceDriverRef;
        satelliteConnectionDriver = satelliteConnectionDriverRef;
        resourceDataDriver = resourceDriverRef;
        storPoolDriver = storPoolDriverRef;
        nodeConnectionDriver = nodeConnectionDriverRef;
    }

    @Override
    public void create(NodeData node, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating Node %s", getId(node));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(node.getUuid()));
            stmt.setString(2, node.getName().value);
            stmt.setString(3, node.getName().displayValue);
            stmt.setLong(4, node.getFlags().getFlagsBits(dbCtx));
            stmt.setLong(5, node.getNodeType(dbCtx).getFlagValue());
            stmt.executeUpdate();

            errorReporter.logTrace("Node created %s", getId(node));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    public List<NodeData> loadAll(TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Loading all Nodes");
        List<NodeData> list = new ArrayList<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    list.add(
                        load(resultSet, transMgr)
                    );
                }
            }
        }
        errorReporter.logTrace("Loaded %d Nodes", list.size());
        return list;
    }

    @Override
    public NodeData load(NodeName nodeName, boolean logWarnIfNotExists, TransactionMgr transMgr)
        throws SQLException
    {
        NodeData node = null;
        errorReporter.logTrace("Loading node %s", getId(nodeName));
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
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning(
                        "Node not found in the DB %s",
                        getId(nodeName)
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
            throw new LinStorSqlRuntimeException(
                String.format(
                    "The display name of a stored Node could not be restored" +
                        "(invalid display NodeName=%s)",
                    resultSet.getString(NODE_DSP_NAME)
                ),
                invalidNameExc
            );
        }

        node = (NodeData) nodesMap.get(nodeName);
        if (node == null)
        {
            node = (NodeData) nodeCache.get(nodeName);
        }
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

            errorReporter.logTrace("Node instance created %s", getId(node));

            // (-> == loads)
            // node -> resource
            // resource -> resourceDefinition
            // resourceDefinition -> resource (other than before)
            // resource -> node (containing "our" node, but also other nodes)
            if (!cacheCleared)
            {
                nodeCache.put(nodeName, node);
            }
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
                    getId(node),
                    netIfaces.size()
                );

                SatelliteConnectionData satelliteConnection = satelliteConnectionDriver.load(node, true, transMgr);
                node.setSatelliteConnection(dbCtx, satelliteConnection);
                errorReporter.logTrace(
                    "Node's SatelliteConnection restored %s",
                    getId(node)
                );

                List<ResourceData> resList = resourceDataDriver.loadResourceData(dbCtx, node, transMgr);
                for (ResourceData res : resList)
                {
                    node.addResource(dbCtx, res);
                }
                errorReporter.logTrace(
                    "Node's Resources restored %s Count: %d",
                    getId(node),
                    resList.size()
                );

                List<StorPoolData> storPoolList = storPoolDriver.loadStorPools(node, transMgr);
                for (StorPoolData storPool : storPoolList)
                {
                    node.addStorPool(dbCtx, storPool);
                }
                errorReporter.logTrace(
                    "Node's StorPools restored %s Count: %d",
                    getId(node),
                    storPoolList.size()
                );

                List<NodeConnectionData> nodeConDfnList =
                    nodeConnectionDriver.loadAllByNode(node, transMgr);
                for (NodeConnectionData nodeConDfn : nodeConDfnList)
                {
                    node.setNodeConnection(dbCtx, nodeConDfn);
                }
                errorReporter.logTrace(
                    "Node's ConnectionDefinitions restored %s Count: %d",
                    getId(node),
                    nodeConDfnList.size()
                );

                errorReporter.logTrace("Node loaded from DB %s", getId(node));
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
        else
        {
            errorReporter.logTrace("Node loaded from cache %s", getId(node));
        }
        return node;
    }

    private ObjectProtection getObjectProtection(NodeName nodeName, TransactionMgr transMgr) throws SQLException
    {
        ObjectProtectionDatabaseDriver objProtDriver = LinStor.getObjectProtectionDatabaseDriver();
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(nodeName),
            false, // no need to log a warning, as we would fail then anyways
            transMgr
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "Node's DB entry exists, but is missing an entry in ObjProt table! " + getId(nodeName),
                null
            );
        }
        return objProt;
    }

    @Override
    public void delete(NodeData node, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting node %s", getId(node));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_DELETE))
        {
            stmt.setString(1, node.getName().value);

            stmt.executeUpdate();
        }
        errorReporter.logTrace("Node deleted %s", getId(node));
    }

    public void clearCache()
    {
        cacheCleared = true;
        nodeCache.clear();
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

    private String getId(NodeData node)
    {
        return getId(node.getName().displayValue);
    }

    private String getId(NodeName nodeName)
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
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        NodeFlag.class,
                        node.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        NodeFlag.class,
                        flags
                    ),
                    ", "
                );

                errorReporter.logTrace("Updating Node's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(node)
                );
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_UPDATE_FLAGS))
                {
                    stmt.setLong(1, flags);
                    stmt.setString(2, node.getName().value);

                    stmt.executeUpdate();
                }
                errorReporter.logTrace("Node's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(node)
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
                    getId(parent)
                );
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NODE_UPDATE_TYPE))
                {
                    stmt.setLong(1, element.getFlagValue());
                    stmt.setString(2, parent.getName().value);

                    stmt.executeUpdate();
                }
                errorReporter.logTrace("Node's NodeType updated from [%s] to [%s] %s",
                    parent.getNodeType(dbCtx).name(),
                    element.name(),
                    getId(parent)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }
}
