package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

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
public class ResourceConnectionDataGenericDbDriver implements ResourceConnectionDataDatabaseDriver
{
    private static final String TBL_RES_CON_DFN = DbConstants.TBL_RESOURCE_CONNECTIONS;

    private static final String UUID = DbConstants.UUID;
    private static final String NODE_SRC = DbConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DbConstants.NODE_NAME_DST;
    private static final String RES_NAME = DbConstants.RESOURCE_NAME;

    private static final String SELECT =
        " SELECT " + UUID + ", " + RES_NAME + ", " + NODE_SRC + ", " + NODE_DST  +
        " FROM " + TBL_RES_CON_DFN +
        " WHERE " + NODE_SRC + " = ? AND " +
                   NODE_DST + " = ? AND " +
                   RES_NAME + " = ?";
    private static final String SELECT_BY_RES_SRC_OR_DST =
        " SELECT " + UUID + ", " + RES_NAME + ", " + NODE_SRC + ", " + NODE_DST  +
        " FROM " + TBL_RES_CON_DFN +
        " WHERE ( " + NODE_SRC + " = ? OR " +
                      NODE_DST + " = ?" +
                   " ) AND " +
                   RES_NAME + " = ?";

    private static final String INSERT =
        " INSERT INTO " + TBL_RES_CON_DFN +
        " (" + UUID + ", " + RES_NAME + ", " + NODE_SRC + ", " + NODE_DST + ")" +
        " VALUES (?, ?, ?, ?)";
    private static final String DELETE =
        " DELETE FROM " + TBL_RES_CON_DFN +
        " WHERE " + NODE_SRC + " = ? AND " +
                   NODE_DST + " = ? AND " +
                   RES_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final Provider<NodeDataGenericDbDriver> nodeDataDbDriverProvider;
    private final Provider<ResourceDataGenericDbDriver> resourceDataDbDriverProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        Provider<NodeDataGenericDbDriver> nodeDataDbDriverProviderRef,
        Provider<ResourceDataGenericDbDriver> resourceDataDbDriverProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        nodeDataDbDriverProvider = nodeDataDbDriverProviderRef;
        resourceDataDbDriverProvider = resourceDataDbDriverProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public ResourceConnectionData load(
        Resource sourceResource,
        Resource targetResource,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading ResourceConnection %s", getId(sourceResource, targetResource));

        ResourceDefinition resDfn = sourceResource.getDefinition();
        if (resDfn != targetResource.getDefinition())
        {
            throw new ImplementationError(
                String.format(
                    "Failed to load ResourceConnection between unrelated resources. %s %s",
                    getResourceTraceId(sourceResource),
                    getResourceTraceId(targetResource)
                ),
                null
            );
        }

        ResourceConnectionData ret = null;
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT))
        {
            stmt.setString(1, sourceResource.getAssignedNode().getName().value);
            stmt.setString(2, targetResource.getAssignedNode().getName().value);
            stmt.setString(3, sourceResource.getDefinition().getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    ret = restoreResourceConnection(resultSet);
                    // traceLog about loaded from DB|cache in restoreConDfn method
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning(
                        "ResourceConnection not found in DB %s",
                        getId(sourceResource, targetResource)
                    );
                }
            }
        }
        return ret;
    }

    private ResourceConnectionData restoreResourceConnection(ResultSet resultSet) throws SQLException
    {
        NodeName sourceNodeName = null;
        NodeName targetNodeName = null;
        ResourceName resourceName = null;
        try
        {
            sourceNodeName = new NodeName(resultSet.getString(NODE_SRC));
            targetNodeName = new NodeName(resultSet.getString(NODE_DST));
            resourceName = new ResourceName(resultSet.getString(RES_NAME));
        }
        catch (InvalidNameException invalidNameExc)
        {
            String col;
            String format = "The stored %s in table %s could not be restored. ";
            if (sourceNodeName == null)
            {
                col = "SourceNodeName";
                format += "(invalid SourceNodeName=%s, TargetNodeName=%s, ResourceName=%s)";
            }
            else
            if (targetNodeName == null)
            {
                col = "TargetNodeName";
                format += "(SourceNodeName=%s, invalid TargetNodeName=%s, ResourceName=%s)";
            }
            else
            {
                col = "ResourceName";
                format += "(SourceNodeName=%s, TargetNodeName=%s, invalid ResourceName=%s)";
            }

            throw new LinStorSqlRuntimeException(
                String.format(
                    format,
                    col,
                    TBL_RES_CON_DFN,
                    resultSet.getString(NODE_SRC),
                    resultSet.getString(NODE_DST),
                    resultSet.getString(RES_NAME)
                ),
                invalidNameExc
            );
        }

        NodeDataGenericDbDriver nodeDataDbDriver = nodeDataDbDriverProvider.get();
        Node sourceNode = nodeDataDbDriver.load(sourceNodeName, true);
        Node targetNode = nodeDataDbDriver.load(targetNodeName, true);

        ResourceDataGenericDbDriver resourceDataDbDriver = resourceDataDbDriverProvider.get();
        Resource sourceResource = resourceDataDbDriver.load(sourceNode, resourceName, true);
        Resource targetResource = resourceDataDbDriver.load(targetNode, resourceName, true);

        ResourceConnectionData resConData = cacheGet(sourceResource, targetResource);
        if (resConData == null)
        {
            try
            {
                resConData = new ResourceConnectionData(
                    java.util.UUID.fromString(resultSet.getString(UUID)),
                    dbCtx,
                    sourceResource,
                    targetResource,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
            }
            errorReporter.logTrace("ResourceConnection loaded from DB %s", getId(resConData));
        }
        else
        {
            errorReporter.logTrace("ResourceConnection loaded from cache %s", getId(resConData));
        }

        return resConData;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public List<ResourceConnectionData> loadAllByResource(Resource resource) throws SQLException
    {
        errorReporter.logTrace(
            "Loading all ResourceConnections for Resource %s",
            getResourceTraceId(resource)
        );

        List<ResourceConnectionData> connections = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_BY_RES_SRC_OR_DST))
        {
            NodeName nodeName = resource.getAssignedNode().getName();
            stmt.setString(1, nodeName.value);
            stmt.setString(2, nodeName.value);
            stmt.setString(3, resource.getDefinition().getName().value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    connections.add(restoreResourceConnection(resultSet));
                }
            }
        }

        errorReporter.logTrace(
            "%d ResourceConnections loaded for Resource %s",
            connections.size(),
            getResourceDebugId(resource)
        );
        return connections;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(ResourceConnectionData conDfnData) throws SQLException
    {
        errorReporter.logTrace("Creating ResourceConnection %s", getId(conDfnData));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            NodeName sourceNodeName = conDfnData.getSourceResource(dbCtx).getAssignedNode().getName();
            NodeName targetNodeName = conDfnData.getTargetResource(dbCtx).getAssignedNode().getName();
            ResourceName resName = conDfnData.getSourceResource(dbCtx).getDefinition().getName();

            stmt.setString(1, conDfnData.getUuid().toString());
            stmt.setString(2, resName.value);
            stmt.setString(3, sourceNodeName.value);
            stmt.setString(4, targetNodeName.value);

            stmt.executeUpdate();

            errorReporter.logTrace("ResourceConnection created s", getId(conDfnData));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(ResourceConnectionData conDfnData) throws SQLException
    {
        errorReporter.logTrace("Deleting ResourceConnection %s", getId(conDfnData));
        try
        {
            NodeName sourceNodeName = conDfnData.getSourceResource(dbCtx).getAssignedNode().getName();
            NodeName targetNodeName = conDfnData.getTargetResource(dbCtx).getAssignedNode().getName();
            ResourceName resName = conDfnData.getSourceResource(dbCtx).getDefinition().getName();

            try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
            {
                stmt.setString(1, sourceNodeName.value);
                stmt.setString(2, targetNodeName.value);
                stmt.setString(3, resName.value);

                stmt.executeUpdate();
            }
            errorReporter.logTrace("ResourceConnection deleted %s", getId(conDfnData));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    private ResourceConnectionData cacheGet(Resource sourceResource, Resource targetResource)
    {
        ResourceConnectionData ret = null;
        try
        {
            ret = (ResourceConnectionData) sourceResource.getResourceConnection(dbCtx, targetResource);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(ResourceConnectionData conData)
    {
        String id = null;
        try
        {
            id = getId(
                conData.getSourceResource(dbCtx).getAssignedNode().getName().displayValue,
                conData.getTargetResource(dbCtx).getAssignedNode().getName().displayValue,
                conData.getSourceResource(dbCtx).getDefinition().getName().displayValue
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
        }
        return id;
    }

    private String getId(Resource src, Resource dst)
    {
        return getId(
            src.getAssignedNode().getName().displayValue,
            dst.getAssignedNode().getName().displayValue,
            src.getDefinition().getName().displayValue
        );
    }

    private String getId(String sourceName, String targetName, String resName)
    {
        return "(SourceNode=" + sourceName + " TargetNode=" + targetName + " ResName=" + resName + ")";
    }

    private String getResourceTraceId(Resource resource)
    {
        return getResourceId(
            resource.getAssignedNode().getName().value,
            resource.getDefinition().getName().value
        );
    }

    private String getResourceDebugId(Resource resource)
    {
        return getResourceId(
            resource.getAssignedNode().getName().displayValue,
            resource.getDefinition().getName().displayValue
        );
    }

    private String getResourceId(String nodeName, String resourceName)
    {
        return "(NodeName=" + nodeName + " ResName=" + resourceName + ")";
    }
}
