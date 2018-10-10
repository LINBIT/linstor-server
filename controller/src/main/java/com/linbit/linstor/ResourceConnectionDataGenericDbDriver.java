package com.linbit.linstor;

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
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

@Singleton
public class ResourceConnectionDataGenericDbDriver implements ResourceConnectionDataDatabaseDriver
{
    private static final String TBL_RES_CON_DFN = DbConstants.TBL_RESOURCE_CONNECTIONS;

    private static final String UUID = DbConstants.UUID;
    private static final String NODE_SRC = DbConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DbConstants.NODE_NAME_DST;
    private static final String RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String FLAGS = "FLAGS";

    private static final String SELECT_ALL =
        " SELECT " + UUID + ", " + RES_NAME + ", " + NODE_SRC + ", " + NODE_DST  + ", " + FLAGS +
        " FROM " + TBL_RES_CON_DFN;

    private static final String INSERT =
        " INSERT INTO " + TBL_RES_CON_DFN +
        " (" + UUID + ", " + RES_NAME + ", " + NODE_SRC + ", " + NODE_DST + ", " + FLAGS + ")" +
        " VALUES (?, ?, ?, ?, ?)";
    private static final String DELETE =
        " DELETE FROM " + TBL_RES_CON_DFN +
        " WHERE " + NODE_SRC + " = ? AND " +
                   NODE_DST + " = ? AND " +
                   RES_NAME + " = ?";
    private static final String RES_UPDATE_FLAG =
        " UPDATE " + TBL_RES_CON_DFN +
            " SET " + FLAGS + " = ? " +
            " WHERE " + NODE_SRC + " = ? AND " + NODE_DST + " = ? AND " +
            RES_NAME      + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final FlagDriver flagDriver;

    @Inject
    public ResourceConnectionDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        flagDriver = new FlagDriver();
    }

    public List<ResourceConnectionData> loadAll(Map<Pair<NodeName, ResourceName>, ? extends Resource> tmpRscMap)
        throws SQLException
    {
        errorReporter.logTrace("Loading all ResourceConnections");
        List<ResourceConnectionData> rscConnections = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    NodeName sourceNodeName;
                    NodeName targetNodeName;
                    ResourceName rscName;
                    long flags;

                    try
                    {
                        sourceNodeName = new NodeName(resultSet.getString(NODE_SRC));
                        targetNodeName = new NodeName(resultSet.getString(NODE_DST));
                        rscName = new ResourceName(resultSet.getString(RES_NAME));
                        flags = resultSet.getLong(FLAGS);
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        throw new ImplementationError(invalidNameExc);
                    }

                    ResourceConnectionData conDfn = restoreResourceConnection(
                        resultSet,
                        tmpRscMap.get(new Pair<>(sourceNodeName, rscName)),
                        tmpRscMap.get(new Pair<>(targetNodeName, rscName)),
                        flags
                    );
                    rscConnections.add(conDfn);
                }
            }
        }
        errorReporter.logTrace("Loaded %d ResourceConnections", rscConnections.size());

        return rscConnections;
    }

    private ResourceConnectionData restoreResourceConnection(
        ResultSet resultSet,
        Resource sourceResource,
        Resource targetResource,
        long flags
    )
        throws SQLException
    {
        ResourceConnectionData resConData = new ResourceConnectionData(
            java.util.UUID.fromString(resultSet.getString(UUID)),
            sourceResource,
            targetResource,
            this,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            flags
        );
        errorReporter.logTrace("ResourceConnection loaded from DB %s", getId(resConData));

        return resConData;
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
            long flags = conDfnData.getStateFlags().getFlagsBits(dbCtx);

            stmt.setString(1, conDfnData.getUuid().toString());
            stmt.setString(2, resName.value);
            stmt.setString(3, sourceNodeName.value);
            stmt.setString(4, targetNodeName.value);
            stmt.setLong(5, flags);

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

    private String getId(String sourceName, String targetName, String resName)
    {
        return "(SourceNode=" + sourceName + " TargetNode=" + targetName + " ResName=" + resName + ")";
    }

    @Override
    public StateFlagsPersistence<ResourceConnectionData> getStateFlagPersistence()
    {
        return flagDriver;
    }

    private class FlagDriver implements StateFlagsPersistence<ResourceConnectionData>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(ResourceConnectionData rscCon, long flags) throws SQLException
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        ResourceConnection.RscConnFlags.class,
                        rscCon.getStateFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        Resource.RscFlags.class,
                        flags
                    ),
                    ", "
                );

                errorReporter.logTrace("Updating Resource connection's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(rscCon)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(RES_UPDATE_FLAG))
                {
                    stmt.setLong(1, flags);

                    stmt.setString(2, rscCon.getSourceResource(dbCtx).getAssignedNode().getName().value);
                    stmt.setString(3, rscCon.getTargetResource(dbCtx).getAssignedNode().getName().value);
                    stmt.setString(4, rscCon.getSourceResource(dbCtx).getDefinition().getName().value);

                    stmt.executeUpdate();

                    errorReporter.logTrace("Resource connection's flags updated from [%s] to [%s] %s",
                        fromFlags,
                        toFlags,
                        getId(rscCon)
                    );
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }
}
