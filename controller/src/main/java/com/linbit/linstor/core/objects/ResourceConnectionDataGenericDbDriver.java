package com.linbit.linstor.core.objects;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionData;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
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
    private static final String RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String NODE_SRC = DbConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DbConstants.NODE_NAME_DST;
    private static final String FLAGS = DbConstants.FLAGS;
    private static final String PORT = DbConstants.TCP_PORT;
    private static final String[] RSC_CON_FIELDS = {
        UUID,
        RES_NAME,
        NODE_SRC,
        NODE_DST,
        FLAGS,
        PORT
    };

    private static final String SELECT_ALL =
        " SELECT " + StringUtils.join(", ", RSC_CON_FIELDS) +
        " FROM " + TBL_RES_CON_DFN;

    private static final String INSERT =
        " INSERT INTO " + TBL_RES_CON_DFN +
        " (" + StringUtils.join(", ", RSC_CON_FIELDS) + ")" +
        " VALUES (" + StringUtils.repeat("?", ", ", RSC_CON_FIELDS.length) + ")";

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

    private static final String RES_UPDATE_PORT =
        " UPDATE " + TBL_RES_CON_DFN +
            " SET " + PORT + " = ? " +
            " WHERE " + NODE_SRC + " = ? AND " + NODE_DST + " = ? AND " +
            RES_NAME      + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final FlagDriver flagDriver;
    private final SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber> portDriver;

    @Inject
    public ResourceConnectionDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        tcpPortPool = tcpPortPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        flagDriver = new FlagDriver();
        portDriver = new PortDriver();
    }

    public List<ResourceConnectionData> loadAll(Map<Pair<NodeName, ResourceName>, ? extends Resource> tmpRscMap)
        throws DatabaseException
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
                    TcpPortNumber port;

                    try
                    {
                        sourceNodeName = new NodeName(resultSet.getString(NODE_SRC));
                        targetNodeName = new NodeName(resultSet.getString(NODE_DST));
                        rscName = new ResourceName(resultSet.getString(RES_NAME));
                        flags = resultSet.getLong(FLAGS);

                        int portInt = resultSet.getInt(PORT);
                        port = resultSet.wasNull() ? null : new TcpPortNumber(portInt);
                    }
                    catch (InvalidNameException | ValueOutOfRangeException exc)
                    {
                        throw new ImplementationError(exc);
                    }

                    ResourceConnectionData conDfn = restoreResourceConnection(
                        resultSet,
                        tmpRscMap.get(new Pair<>(sourceNodeName, rscName)),
                        tmpRscMap.get(new Pair<>(targetNodeName, rscName)),
                        flags,
                        port
                    );
                    rscConnections.add(conDfn);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d ResourceConnections", rscConnections.size());

        return rscConnections;
    }

    private ResourceConnectionData restoreResourceConnection(
        ResultSet resultSet,
        Resource sourceResource,
        Resource targetResource,
        long flags,
        TcpPortNumber port
    )
        throws DatabaseException
    {
        try {
            ResourceConnectionData resConData = new ResourceConnectionData(
                java.util.UUID.fromString(resultSet.getString(UUID)),
                sourceResource,
                targetResource,
                port,
                tcpPortPool,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                flags
            );
            errorReporter.logTrace("ResourceConnection loaded from DB %s", getId(resConData));
            return resConData;
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(ResourceConnectionData conDfnData) throws DatabaseException
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

            TcpPortNumber port = conDfnData.getPort(dbCtx);
            if (port == null)
            {
                stmt.setNull(6, Types.INTEGER);
            }
            else
            {
                stmt.setInt(6, port.value);
            }

            stmt.executeUpdate();

            errorReporter.logTrace("ResourceConnection created s", getId(conDfnData));
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
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(ResourceConnectionData conDfnData) throws DatabaseException
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
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace("ResourceConnection deleted %s", getId(conDfnData));
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
            DatabaseLoader.handleAccessDeniedException(accDeniedExc);
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

    @Override
    public SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber> getPortDriver()
    {
        return portDriver;
    }

    private class FlagDriver implements StateFlagsPersistence<ResourceConnectionData>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(ResourceConnectionData rscCon, long flags) throws DatabaseException
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
                catch (SQLException sqlExc)
                {
                    throw new DatabaseException(sqlExc);
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class PortDriver implements SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(ResourceConnectionData rscCon, TcpPortNumber port)
            throws DatabaseException
        {
            try
            {
                TcpPortNumber fromPort = rscCon.getPort(dbCtx);

                errorReporter.logTrace("Updating Resource connection's port from [%d] to [%d] %s",
                    TcpPortNumber.getValueNullable(fromPort),
                    TcpPortNumber.getValueNullable(port),
                    getId(rscCon)
                );

                try (PreparedStatement stmt = getConnection().prepareStatement(RES_UPDATE_PORT))
                {
                    if (port == null)
                    {
                        stmt.setNull(1, Types.INTEGER);
                    }
                    else
                    {
                        stmt.setInt(1, port.value);
                    }
                    stmt.setString(2, rscCon.getSourceResource(dbCtx).getAssignedNode().getName().value);
                    stmt.setString(3, rscCon.getTargetResource(dbCtx).getAssignedNode().getName().value);
                    stmt.setString(4, rscCon.getSourceResource(dbCtx).getDefinition().getName().value);

                    stmt.executeUpdate();
                }
                catch (SQLException sqlExc)
                {
                    throw new DatabaseException(sqlExc);
                }

                errorReporter.logTrace("Resource connection's port updated from [%d] to [%d] %s",
                    TcpPortNumber.getValueNullable(fromPort),
                    TcpPortNumber.getValueNullable(port),
                    getId(rscCon)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accDeniedExc);
            }
        }
    }
}
