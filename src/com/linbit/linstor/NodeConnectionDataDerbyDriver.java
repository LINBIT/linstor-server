package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.UuidUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class NodeConnectionDataDerbyDriver implements NodeConnectionDataDatabaseDriver
{
    private static final String TBL_NODE_CON_DFN = DerbyConstants.TBL_NODE_CONNECTIONS;

    private static final String UUID = DerbyConstants.UUID;
    private static final String NODE_SRC = DerbyConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DerbyConstants.NODE_NAME_DST;

    private static final String SELECT =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST  +
        " FROM " + TBL_NODE_CON_DFN +
        " WHERE " + NODE_SRC + " = ? AND " +
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
        " WHERE " + NODE_SRC + " = ? AND " +
                   NODE_DST + " = ?";


    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final Provider<NodeDataDerbyDriver> nodeDriverProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NodeConnectionDataDerbyDriver(
        @SystemContext AccessContext privCtx,
        ErrorReporter errorReporterRef,
        Provider<NodeDataDerbyDriver> nodeDriverProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        nodeDriverProvider = nodeDriverProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    public NodeConnectionData load(
        Node sourceNode,
        Node targetNode,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading NodeConnection %s", getId(sourceNode, targetNode));

        NodeConnectionData ret = null;
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT))
        {
            stmt.setString(1, sourceNode.getName().value);
            stmt.setString(2, targetNode.getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    ret = restoreNodeConnection(resultSet);
                    // traceLog about loaded from DB|cache in restoreConDfn method
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning(
                        "NodeConnection not found in DB %s",
                        getId(sourceNode, targetNode)
                    );
                }
            }
        }
        return ret;
    }

    @Override
    public List<NodeConnectionData> loadAllByNode(Node node) throws SQLException
    {
        errorReporter.logTrace(
            "Loading all NodeConnections for Node %s",
            getNodeTraceId(node)
        );

        List<NodeConnectionData> nodeConnections = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_BY_NODE_SRC_OR_DST))
        {
            NodeName nodeName = node.getName();
            stmt.setString(1, nodeName.value);
            stmt.setString(2, nodeName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    NodeConnectionData conDfn = restoreNodeConnection(resultSet);
                    nodeConnections.add(conDfn);
                }
            }
        }

        errorReporter.logTrace(
            "%d NodeConnections loaded for Node %s",
            nodeConnections.size(),
            getNodeDebugId(node)
        );
        return nodeConnections;
    }

    private NodeConnectionData restoreNodeConnection(ResultSet resultSet)
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

        NodeDataDerbyDriver nodeDriver = nodeDriverProvider.get();
        Node sourceNode = nodeDriver.load(sourceNodeName, true);
        Node targetNode = nodeDriver.load(targetNodeName, true);

        NodeConnectionData nodeConData = cacheGet(sourceNode, targetNode);
        if (nodeConData == null)
        {
            try
            {
                nodeConData = new NodeConnectionData(
                    java.util.UUID.fromString(resultSet.getString(UUID)),
                    dbCtx,
                    sourceNode,
                    targetNode,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
            errorReporter.logTrace("ResourceConnection loaded from DB %s", getId(nodeConData));
        }
        else
        {
            errorReporter.logTrace("ResourceConnection loaded from cache %s", getId(nodeConData));
        }

        return nodeConData;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(NodeConnectionData nodeConDfnData) throws SQLException
    {
        errorReporter.logTrace("Creating NodeConnection %s", getId(nodeConDfnData));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            NodeName sourceNodeName = nodeConDfnData.getSourceNode(dbCtx).getName();
            NodeName targetNodeName = nodeConDfnData.getTargetNode(dbCtx).getName();

            stmt.setString(1, nodeConDfnData.getUuid().toString());
            stmt.setString(2, sourceNodeName.value);
            stmt.setString(3, targetNodeName.value);

            stmt.executeUpdate();

            errorReporter.logTrace("NodeConnection created s", getId(nodeConDfnData));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public void delete(NodeConnectionData nodeConDfnData) throws SQLException
    {
        errorReporter.logTrace("Deleting NodeConnection %s", getId(nodeConDfnData));
        try
        {
            NodeName sourceNodeName = nodeConDfnData.getSourceNode(dbCtx).getName();
            NodeName targetNodeName = nodeConDfnData.getTargetNode(dbCtx).getName();

            try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
            {
                stmt.setString(1, sourceNodeName.value);
                stmt.setString(2, targetNodeName.value);

                stmt.executeUpdate();
            }
            errorReporter.logTrace("NodeConnection deleted %s", getId(nodeConDfnData));
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

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    /*
     * Debug ID methods
     */
    private String getId(NodeConnectionData conData)
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

    private String getId(Node src, Node dst)
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
