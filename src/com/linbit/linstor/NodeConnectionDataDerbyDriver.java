package com.linbit.linstor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

public class NodeConnectionDataDerbyDriver implements NodeConnectionDataDatabaseDriver
{
    private static final String TBL_NODE_CON_DFN = DerbyConstants.TBL_NODE_CONNECTIONS;

    private static final String UUID = DerbyConstants.UUID;
    private static final String NODE_SRC = DerbyConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DerbyConstants.NODE_NAME_DST;

    private static final String SELECT =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST  +
        " FROM " + TBL_NODE_CON_DFN +
        " WHERE "+ NODE_SRC + " = ? AND " +
                   NODE_DST + " = ?";
    private static final String SELECT_BY_NODE_SRC_OR_DST =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST  +
        " FROM  " + TBL_NODE_CON_DFN +
        " WHERE " + NODE_SRC + " = ? OR " +
                    NODE_DST + " = ?";

    private static final String INSERT =
        " INSERT INTO " + TBL_NODE_CON_DFN +
        " (" + UUID + ", " + NODE_SRC + ", " + NODE_DST  + ")" +
        " VALUES (?, ?, ?)";
    private static final String DELETE =
        " DELETE FROM " + TBL_NODE_CON_DFN +
        " WHERE "+ NODE_SRC + " = ? AND " +
                   NODE_DST + " = ?";


    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private NodeDataDerbyDriver nodeDriver;

    public NodeConnectionDataDerbyDriver(
        AccessContext privCtx,
        ErrorReporter errorReporterRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
    }

    public void initialize(NodeDataDerbyDriver nodeDataDerbyDriverRef)
    {
        nodeDriver = nodeDataDerbyDriverRef;
    }

    @Override
    public NodeConnectionData load(
        Node sourceNode,
        Node targetNode,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading NodeConnection %s", getTraceId(sourceNode, targetNode));

        NodeConnectionData ret = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT))
        {
            stmt.setString(1, sourceNode.getName().value);
            stmt.setString(2, targetNode.getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    ret = restoreNodeConnection(resultSet, transMgr);
                    // traceLog about loaded from DB|cache in restoreConDfn method
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning(
                        "NodeConnection not found in DB %s",
                        getDebugId(sourceNode, targetNode)
                    );
                }
            }
        }
        return ret;
    }

    @Override
    public List<NodeConnectionData> loadAllByNode(
        Node node,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace(
            "Loading all NodeConnections for Node %s",
            getNodeTraceId(node)
        );

        List<NodeConnectionData> connections = new ArrayList<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_BY_NODE_SRC_OR_DST))
        {
            NodeName nodeName = node.getName();
            stmt.setString(1, nodeName.value);
            stmt.setString(2, nodeName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    NodeConnectionData conDfn = restoreNodeConnection(
                        resultSet,
                        transMgr
                    );
                    connections.add(conDfn);
                }
            }
        }

        errorReporter.logTrace(
            "%d NodeConnections loaded for Node %s",
            connections.size(),
            getNodeDebugId(node)
        );
        return connections;
    }

    private NodeConnectionData restoreNodeConnection(
        ResultSet resultSet,
        TransactionMgr transMgr
    )
        throws SQLException
    {

        NodeName sourceNodeName = null;
        NodeName targetNodeName = null;
        try
        {
            sourceNodeName = new NodeName(resultSet.getString(NODE_SRC));
            targetNodeName = new NodeName(resultSet.getString(NODE_DST));
        }
        catch (InvalidNameException invalidNameExc)
        {
            String col;
            String format = "The stored %s in table %s could not be restored. ";
            if (sourceNodeName == null)
            {
                col = "SourceNodeName";
                format += "(invalid SourceNodeName=%s, TargetNodeName=%s)";
            }
            else
            {
                col = "TargetNodeName";
                format += "(SourceNodeName=%s, invalid TargetNodeName=%s)";
            }

            throw new LinStorSqlRuntimeException(
                String.format(
                    format,
                    col,
                    TBL_NODE_CON_DFN,
                    resultSet.getString(NODE_SRC),
                    resultSet.getString(NODE_DST)
                ),
                invalidNameExc
            );
        }


        Node sourceNode = nodeDriver.load(sourceNodeName, true, transMgr);
        Node targetNode = nodeDriver.load(targetNodeName, true, transMgr);

        NodeConnectionData nodeConData = cacheGet(sourceNode, targetNode);
        if (nodeConData == null)
        {
            try
            {
                nodeConData = new NodeConnectionData(
                    UuidUtils.asUuid(resultSet.getBytes(UUID)),
                    dbCtx,
                    sourceNode,
                    targetNode,
                    transMgr
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
            errorReporter.logTrace("ResourceConnection loaded from DB %s", getDebugId(nodeConData));
        }
        else
        {
            errorReporter.logTrace("ResourceConnection loaded from cache %s", getDebugId(nodeConData));
        }

        return nodeConData;
    }

    @Override
    public void create(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating NodeConnection %s", getTraceId(nodeConDfnData));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT))
        {
            NodeName sourceNodeName = nodeConDfnData.getSourceNode(dbCtx).getName();
            NodeName targetNodeName = nodeConDfnData.getTargetNode(dbCtx).getName();

            stmt.setBytes(1, UuidUtils.asByteArray(nodeConDfnData.getUuid()));
            stmt.setString(2, sourceNodeName.value);
            stmt.setString(3, targetNodeName.value);

            stmt.executeUpdate();

            errorReporter.logTrace("NodeConnection created s", getDebugId(nodeConDfnData));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public void delete(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting NodeConnection %s", getTraceId(nodeConDfnData));
        try
        {
            NodeName sourceNodeName = nodeConDfnData.getSourceNode(dbCtx).getName();
            NodeName targetNodeName = nodeConDfnData.getTargetNode(dbCtx).getName();

            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(DELETE))
            {
                stmt.setString(1, sourceNodeName.value);
                stmt.setString(2, targetNodeName.value);

                stmt.executeUpdate();
            }
            errorReporter.logTrace("NodeConnection deleted %s", getDebugId(nodeConDfnData));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    private NodeConnectionData cacheGet(Node sourceNode, Node targetNode)
    {
        NodeConnectionData ret = null;
        try
        {
            ret = (NodeConnectionData) sourceNode.getNodeConnection(dbCtx, targetNode);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    /*
     * Trace and Debug ID methods
     */
    private String getTraceId(NodeConnectionData conData)
    {
        String id = null;
        try
        {
            id = getId(
                conData.getSourceNode(dbCtx).getName().value,
                conData.getTargetNode(dbCtx).getName().value
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return id;
    }

    private String getDebugId(NodeConnectionData conData)
    {
        String id = null;
        try
        {
            id = getId(
                conData.getSourceNode(dbCtx).getName().displayValue,
                conData.getTargetNode(dbCtx).getName().displayValue
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return id;
    }

    private String getTraceId(Node src, Node dst)
    {
        return getId(
            src.getName().value,
            dst.getName().value
        );
    }

    private String getDebugId(Node src, Node dst)
    {
        return getId(
            src.getName().displayValue,
            dst.getName().displayValue
        );
    }

    private String getId(String sourceName, String targetName)
    {
        return "(SourceNode=" + sourceName + " TargetNode=" + targetName + ")";
    }

    private String getNodeTraceId(Node node)
    {
        return getNodeId(node.getName().value);
    }

    private String getNodeDebugId(Node node)
    {
        return getNodeId(node.getName().displayValue);
    }

    private String getNodeId(String name)
    {
        return "(NodeName=" + name + ")";
    }
}
