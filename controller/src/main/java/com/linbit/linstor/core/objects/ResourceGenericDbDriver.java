package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class ResourceGenericDbDriver implements ResourceDatabaseDriver
{
    private static final String TBL_RES = DbConstants.TBL_RESOURCES;

    private static final String RES_UUID = DbConstants.UUID;
    private static final String RES_NODE_NAME = DbConstants.NODE_NAME;
    private static final String RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String RES_FLAGS = DbConstants.RESOURCE_FLAGS;

    private static final String RES_SELECT_ALL =
        " SELECT " + RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " +
                     RES_FLAGS +
        " FROM " + TBL_RES;
    private static final String RES_SELECT =
        RES_SELECT_ALL +
        " WHERE " + RES_NODE_NAME + " = ? AND " +
            RES_NAME      + " = ?";

    private static final String RES_INSERT =
        " INSERT INTO " + TBL_RES +
        " (" +
            RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " +
            RES_FLAGS +
        ") VALUES (?, ?, ?, ?)";
    private static final String RES_DELETE =
        " DELETE FROM " + TBL_RES +
        " WHERE " + RES_NODE_NAME + " = ? AND " +
                    RES_NAME      + " = ?";
    private static final String RES_UPDATE_FLAG =
        " UPDATE " + TBL_RES +
        " SET " + RES_FLAGS + " = ? " +
        " WHERE " + RES_NODE_NAME + " = ? AND " +
                    RES_NAME      + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final FlagDriver flagDriver;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public ResourceGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        flagDriver = new FlagDriver();
    }

    @Override
    public void create(Resource res) throws DatabaseException
    {
        create(dbCtx, res);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void create(AccessContext accCtx, Resource res) throws DatabaseException
    {
        errorReporter.logTrace("Creating Resource %s", getId(res));
        try (PreparedStatement stmt = getConnection().prepareStatement(RES_INSERT))
        {
            stmt.setString(1, res.getUuid().toString());
            stmt.setString(2, res.getNode().getName().value);
            stmt.setString(3, res.getDefinition().getName().value);
            stmt.setLong(4, res.getStateFlags().getFlagsBits(accCtx));
            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
        errorReporter.logTrace("Resource created %s", getId(res));
    }

    public void ensureResExists(AccessContext accCtx, Resource res)
        throws DatabaseException
    {
        errorReporter.logTrace("Ensuring Resource exists %s", getId(res));
        try (PreparedStatement stmt = getConnection().prepareStatement(RES_SELECT))
        {
            stmt.setString(1, res.getNode().getName().value);
            stmt.setString(2, res.getDefinition().getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (!resultSet.next())
                {
                    create(accCtx, res);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }


    public Map<Resource, Resource.InitMaps> loadAll(
        Map<NodeName, ? extends Node> nodesMap,
        Map<ResourceName, ? extends ResourceDefinition> rscDfnMap
    )
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all Resources");
        Map<Resource, Resource.InitMaps> loadedResources = new TreeMap<>();
        String nodeNameStr;
        String rscNameStr;
        try (PreparedStatement stmt = getConnection().prepareStatement(RES_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    try
                    {
                        nodeNameStr = resultSet.getString(RES_NODE_NAME);
                        NodeName nodeName = new NodeName(nodeNameStr);
                        rscNameStr = resultSet.getString(RES_NAME);
                        ResourceName rscName = new ResourceName(rscNameStr);

                        Pair<Resource, Resource.InitMaps> pair = restoreRsc(
                            resultSet,
                            nodesMap.get(nodeName),
                            rscDfnMap.get(rscName)
                        );
                        loadedResources.put(
                            pair.objA,
                            pair.objB
                        );
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            "Invalid name restored from database: " + exc.invalidName,
                            exc
                        );
                    }
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d Resources", loadedResources.size());
        return loadedResources;
    }

    private Pair<Resource, Resource.InitMaps> restoreRsc(
        ResultSet resultSet,
        Node node,
        ResourceDefinition rscDfn
    )
        throws DatabaseException
    {
        Map<Resource.ResourceKey, ResourceConnection> rscConnMap = new TreeMap<>();
        Map<VolumeNumber, Volume> vlmMap = new TreeMap<>();
        ResourceInitMaps initMaps = new ResourceInitMaps(rscConnMap, vlmMap);

        Resource rscData;
        try
        {
            rscData = new Resource(
                UUID.fromString(resultSet.getString(RES_UUID)),
                getObjectProection(node, rscDfn.getName()),
                rscDfn,
                node,
                resultSet.getLong(RES_FLAGS),
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                rscConnMap,
                vlmMap
            );
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return new Pair<Resource, Resource.InitMaps>(rscData, initMaps);
    }

    private ObjectProtection getObjectProection(Node node, ResourceName resName)
        throws DatabaseException
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(node.getName(), resName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "Resource's DB entry exists, but is missing an entry in ObjProt table! " + getId(node, resName),
                null
            );
        }
        return objProt;
    }

    @Override
    public void delete(Resource resource) throws DatabaseException
    {
        errorReporter.logTrace("Deleting Resource %s", getId(resource));
        try (PreparedStatement stmt = getConnection().prepareStatement(RES_DELETE))
        {
            stmt.setString(1, resource.getNode().getName().value);
            stmt.setString(2, resource.getDefinition().getName().value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Resource deleted %s", getId(resource));
    }

    @Override
    public StateFlagsPersistence<Resource> getStateFlagPersistence()
    {
        return flagDriver;
    }

    private String getId(Node node, ResourceName resourceName)
    {
        return getId(
            node.getName().displayValue,
            resourceName.displayValue
        );
    }

    private String getId(Resource res)
    {
        return getId(
            res.getNode().getName().displayValue,
            res.getDefinition().getName().displayValue
        );
    }

    private String getId(String nodeName, String resName)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + ")";
    }

    private class FlagDriver implements StateFlagsPersistence<Resource>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(Resource resource, long flags) throws DatabaseException
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        Resource.Flags.class,
                        resource.getStateFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        Resource.Flags.class,
                        flags
                    ),
                    ", "
                );

                errorReporter.logTrace("Updating Resource's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(resource)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(RES_UPDATE_FLAG))
                {
                    stmt.setLong(1, flags);

                    stmt.setString(2, resource.getNode().getName().value);
                    stmt.setString(3, resource.getDefinition().getName().value);

                    stmt.executeUpdate();

                    errorReporter.logTrace("Resource's flags updated from [%s] to [%s] %s",
                        fromFlags,
                        toFlags,
                        getId(resource)
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

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private class ResourceInitMaps implements Resource.InitMaps
    {
        private final Map<Resource.ResourceKey, ResourceConnection> rscConnMap;
        private final Map<VolumeNumber, Volume> vlmMap;

        ResourceInitMaps(
            Map<Resource.ResourceKey, ResourceConnection> rscConnMapRef,
            Map<VolumeNumber, Volume> vlmMapRef
        )
        {
            rscConnMap = rscConnMapRef;
            vlmMap = vlmMapRef;
        }

        @Override
        public Map<Resource.ResourceKey, ResourceConnection> getRscConnMap()
        {
            return rscConnMap;
        }

        @Override
        public Map<VolumeNumber, Volume> getVlmMap()
        {
            return vlmMap;
        }

    }
}
