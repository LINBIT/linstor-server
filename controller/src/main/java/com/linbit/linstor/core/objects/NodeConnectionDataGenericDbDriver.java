package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class NodeConnectionDataGenericDbDriver implements NodeConnectionDataDatabaseDriver
{
    private static final String TBL_NODE_CON_DFN = DbConstants.TBL_NODE_CONNECTIONS;
    private static final String UUID = DbConstants.UUID;
    private static final String NODE_SRC = DbConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DbConstants.NODE_NAME_DST;
    private static final String[] NODE_CON_FIELDS = {
        UUID,
        NODE_SRC,
        NODE_DST
    };

    private static final String SELECT_ALL =
        " SELECT " + StringUtils.join(", ", NODE_CON_FIELDS) +
        " FROM " + TBL_NODE_CON_DFN;

    private static final String INSERT =
        " INSERT INTO " + TBL_NODE_CON_DFN +
        " (" + StringUtils.join(", ", NODE_CON_FIELDS)  + ")" +
        " VALUES (" + StringUtils.repeat("?", ", ", NODE_CON_FIELDS.length) + ")";

    private static final String DELETE =
        " DELETE FROM " + TBL_NODE_CON_DFN +
        " WHERE " + NODE_SRC + " = ? AND " +
                   NODE_DST + " = ?";


    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public NodeConnectionDataGenericDbDriver(
        @SystemContext AccessContext privCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public List<NodeConnectionData> loadAll(Map<NodeName, ? extends Node> tmpNodesMap)
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all NodeConnections");
        List<NodeConnectionData> nodeConnections = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
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
                        throw new ImplementationError(invalidNameExc);
                    }

                    NodeConnectionData conDfn = restoreNodeConnection(
                        resultSet,
                        tmpNodesMap.get(sourceNodeName),
                        tmpNodesMap.get(targetNodeName)
                    );
                    nodeConnections.add(conDfn);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        return nodeConnections;
    }

    private NodeConnectionData restoreNodeConnection(
        ResultSet resultSet,
        Node sourceNode,
        Node targetNode
    )
        throws DatabaseException
    {
        try {
            return new NodeConnectionData(
                java.util.UUID.fromString(resultSet.getString(UUID)),
                sourceNode,
                targetNode,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(NodeConnectionData nodeConDfnData) throws DatabaseException
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public void delete(NodeConnectionData nodeConDfnData) throws DatabaseException
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accDeniedExc);
        }
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
            DatabaseLoader.handleAccessDeniedException(accDeniedExc);
        }
        return id;
    }

    private String getId(String sourceName, String targetName)
    {
        return "(SourceNode=" + sourceName + " TargetNode=" + targetName + ")";
    }
}
