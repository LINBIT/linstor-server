package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.Resource.InitMaps;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.StringUtils;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class ResourceDataGenericDbDriver implements ResourceDataDatabaseDriver
{
    private static final String TBL_RES = DbConstants.TBL_RESOURCES;

    private static final String RES_UUID = DbConstants.UUID;
    private static final String RES_NODE_NAME = DbConstants.NODE_NAME;
    private static final String RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String RES_NODE_ID = DbConstants.NODE_ID;
    private static final String RES_FLAGS = DbConstants.RESOURCE_FLAGS;

    private static final String RES_SELECT_ALL =
        " SELECT " + RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " +
                     RES_NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_RES;
    private static final String RES_SELECT =
        RES_SELECT_ALL +
        " WHERE " + RES_NODE_NAME + " = ? AND " +
            RES_NAME      + " = ?";

    private static final String RES_INSERT =
        " INSERT INTO " + TBL_RES +
        " (" +
            RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " +
            RES_NODE_ID + ", " + RES_FLAGS +
        ") VALUES (?, ?, ?, ?, ?)";
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
    private final VolumeDataFactory volumeDataFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;


    @Inject
    public ResourceDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        volumeDataFactory = volumeDataFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        flagDriver = new FlagDriver();
    }

    @Override
    public void create(ResourceData res) throws SQLException
    {
        create(dbCtx, res);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void create(AccessContext accCtx, ResourceData res) throws SQLException
    {
        errorReporter.logTrace("Creating Resource %s", getId(res));
        try (PreparedStatement stmt = getConnection().prepareStatement(RES_INSERT))
        {
            stmt.setString(1, res.getUuid().toString());
            stmt.setString(2, res.getAssignedNode().getName().value);
            stmt.setString(3, res.getDefinition().getName().value);
            stmt.setInt(4, res.getNodeId().value);
            stmt.setLong(5, res.getStateFlags().getFlagsBits(accCtx));
            stmt.executeUpdate();
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
        }
        errorReporter.logTrace("Resource created %s", getId(res));
    }

    public void ensureResExists(AccessContext accCtx, ResourceData res)
        throws SQLException
    {
        errorReporter.logTrace("Ensuring Resource exists %s", getId(res));
        try (PreparedStatement stmt = getConnection().prepareStatement(RES_SELECT))
        {
            stmt.setString(1, res.getAssignedNode().getName().value);
            stmt.setString(2, res.getDefinition().getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (!resultSet.next())
                {
                    create(accCtx, res);
                }
            }
        }
    }


    public Map<ResourceData, Resource.InitMaps> loadAll(
        Map<NodeName, ? extends Node> nodesMap,
        Map<ResourceName, ? extends ResourceDefinition> rscDfnMap
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading all Resources");
        Map<ResourceData, Resource.InitMaps> loadedResources = new TreeMap<>();
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

                        Pair<ResourceData, InitMaps> pair = restoreRsc(
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
        errorReporter.logTrace("Loaded %d Resources", loadedResources.size());
        return loadedResources;
    }

    @Override
    public ResourceData load(Node node, ResourceDefinition rscDfn, boolean logWarnIfNotExists)
        throws SQLException
    {
        ResourceName rscName = rscDfn.getName();
        errorReporter.logTrace("Loading Resource %s", getId(node, rscName));

        ResourceData ret = cacheGet(node, rscName);
        if (ret == null)
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(RES_SELECT))
            {
                stmt.setString(1, node.getName().value);
                stmt.setString(2, rscName.value);
                try (ResultSet resultSet = stmt.executeQuery())
                {
                    if (resultSet.next())
                    {
                        ret = restoreRsc(resultSet, node, rscDfn).objA;
                        if (ret != null)
                        {
                            errorReporter.logTrace("Resource %s loaded from Database", getId(node, rscName));

                        }
                    }
                    if (ret == null && logWarnIfNotExists)
                    {
                        errorReporter.logWarning("Resource could not be found %s", getId(node, rscName));
                    }
                }
            }
        }
        return ret;
    }

    private Pair<ResourceData, InitMaps> restoreRsc(
        ResultSet resultSet,
        Node node,
        ResourceDefinition rscDfn
    )
        throws SQLException
    {
        NodeId nodeId = getNodeId(resultSet, node, rscDfn);

        Map<Resource, ResourceConnection> rscConnMap = new TreeMap<>();
        Map<VolumeNumber, Volume> vlmMap = new TreeMap<>();
        ResourceInitMaps initMaps = new ResourceInitMaps(rscConnMap, vlmMap);

        ResourceData rscData = new ResourceData(
            java.util.UUID.fromString(resultSet.getString(RES_UUID)),
            getObjectProection(node, rscDfn.getName()),
            rscDfn,
            node,
            nodeId,
            resultSet.getLong(RES_FLAGS),
            this,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            rscConnMap,
            vlmMap
        );
        return new Pair<ResourceData, InitMaps>(rscData, initMaps);
    }

    private NodeId getNodeId(ResultSet resultSet, Node node, ResourceDefinition rscDfn)
        throws SQLException
    {
        NodeId nodeId;
        try
        {
            nodeId = new NodeId(resultSet.getInt(RES_NODE_ID));
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "A NodeId of a stored Resource in the table %s could not be restored. " +
                        "(NodeName=%s, ResName=%s, invalid NodeId=%d)",
                    TBL_RES,
                    node.getName().displayValue,
                    rscDfn.getName().displayValue,
                    resultSet.getInt(RES_NODE_ID)
                ),
                valueOutOfRangeExc
            );
        }
        return nodeId;
    }


    private ObjectProtection getObjectProection(Node node, ResourceName resName)
        throws SQLException
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
    public void delete(ResourceData resource) throws SQLException
    {
        errorReporter.logTrace("Deleting Resource %s", getId(resource));
        try (PreparedStatement stmt = getConnection().prepareStatement(RES_DELETE))
        {
            stmt.setString(1, resource.getAssignedNode().getName().value);
            stmt.setString(2, resource.getDefinition().getName().value);

            stmt.executeUpdate();
        }
        errorReporter.logTrace("Resource deleted %s", getId(resource));
    }

    @Override
    public StateFlagsPersistence<ResourceData> getStateFlagPersistence()
    {
        return flagDriver;
    }

    private ResourceData cacheGet(Node node, ResourceName resName)
    {
        ResourceData ret = null;
        try
        {
            ret = (ResourceData) node.getResource(dbCtx, resName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private String getId(Node node, ResourceName resourceName)
    {
        return getId(
            node.getName().displayValue,
            resourceName.displayValue
        );
    }

    private String getId(ResourceData res)
    {
        return getId(
            res.getAssignedNode().getName().displayValue,
            res.getDefinition().getName().displayValue
        );
    }

    private String getId(String nodeName, String resName)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + ")";
    }

    private class FlagDriver implements StateFlagsPersistence<ResourceData>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(ResourceData resource, long flags) throws SQLException
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        RscFlags.class,
                        resource.getStateFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        RscFlags.class,
                        flags
                    ),
                    ", "
                );

                errorReporter.logTrace("Updating Reource's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(resource)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(RES_UPDATE_FLAG))
                {
                    stmt.setLong(1, flags);

                    stmt.setString(2, resource.getAssignedNode().getName().value);
                    stmt.setString(3, resource.getDefinition().getName().value);

                    stmt.executeUpdate();

                    errorReporter.logTrace("Reource's flags updated from [%s] to [%s] %s",
                        fromFlags,
                        toFlags,
                        getId(resource)
                    );
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private class ResourceInitMaps implements Resource.InitMaps
    {
        private final Map<Resource, ResourceConnection> rscConnMap;
        private final Map<VolumeNumber, Volume> vlmMap;

        ResourceInitMaps(
            Map<Resource, ResourceConnection> rscConnMapRef,
            Map<VolumeNumber, Volume> vlmMapRef
        )
        {
            rscConnMap = rscConnMapRef;
            vlmMap = vlmMapRef;
        }

        @Override
        public Map<Resource, ResourceConnection> getRscConnMap()
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
