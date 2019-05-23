package com.linbit.linstor.core.objects;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceGroup.InitMaps;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.ALLOWED_PROVIDER_LIST;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DESCRIPTION;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_KIND_STACK;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DO_NOT_PLACE_WITH_RSC_REGEX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DO_NOT_PLACE_WITH_RSC_LIST;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.POOL_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.REPLICA_COUNT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.REPLICAS_ON_DIFFERENT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.REPLICAS_ON_SAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_GROUP_DSP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_GROUP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_RESOURCE_GROUPS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class ResourceGroupDataGenericDbDriver implements ResourceGroupDataDatabaseDriver
{
    private static final String[] RSC_GRP_FIELDS =
    {
        UUID,
        RESOURCE_GROUP_NAME,
        RESOURCE_GROUP_DSP_NAME,
        DESCRIPTION,
        LAYER_KIND_STACK,
        ALLOWED_PROVIDER_LIST,
        REPLICA_COUNT,
        POOL_NAME,
        DO_NOT_PLACE_WITH_RSC_REGEX,
        DO_NOT_PLACE_WITH_RSC_LIST,
        REPLICAS_ON_SAME,
        REPLICAS_ON_DIFFERENT
    };

    private static final String SELECT_ALL_RSC_GRPS =
        " SELECT " + StringUtils.join(", ", RSC_GRP_FIELDS) +
        " FROM " + TBL_RESOURCE_GROUPS;

    private static final String INSERT =
        " INSERT INTO " + TBL_RESOURCE_GROUPS +
        " ( " + StringUtils.join(", ", RSC_GRP_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", RSC_GRP_FIELDS.length) + " )";

    private static final String UPDATE_DESCR =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + DESCRIPTION + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_LAYER_STACK =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + LAYER_KIND_STACK + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_AP_REPLICA_COUNT =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + REPLICA_COUNT + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_AP_STOR_POOL_NAME =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + POOL_NAME + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_AP_DO_NOT_PLACE_WITH_REGEX =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + DO_NOT_PLACE_WITH_RSC_REGEX + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_AP_DO_NOT_PLACE_WITH_LIST =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + DO_NOT_PLACE_WITH_RSC_LIST + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_AP_REPLICAS_ON_SAME =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + REPLICAS_ON_SAME + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_AP_REPLICAS_ON_DIFFERENT =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + REPLICAS_ON_DIFFERENT + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";
    private static final String UPDATE_AP_ALLOWED_PROVIDER_LIST =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + REPLICAS_ON_DIFFERENT + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";

    private static final String DELETE =
        " DELETE FROM " + TBL_RESOURCE_GROUPS +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";


    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final SingleColumnDatabaseDriver<ResourceGroupData, String> descriptionDriver;
    private final CollectionDatabaseDriver<ResourceGroupData, DeviceLayerKind> layerStackDriver;
    private final SingleColumnDatabaseDriver<ResourceGroupData, Integer> replicaCountDriver;
    private final SingleColumnDatabaseDriver<ResourceGroupData, String>  storPoolNameDriver;
    private final CollectionDatabaseDriver<ResourceGroupData, String> doNotPlaceWithRscListDriver;
    private final SingleColumnDatabaseDriver<ResourceGroupData, String> doNotPlaceWithRscRegexDriver;
    private final CollectionDatabaseDriver<ResourceGroupData, String> replicasOnSameListDriver;
    private final CollectionDatabaseDriver<ResourceGroupData, String> replicasOnDifferentListDriver;
    private final CollectionDatabaseDriver<ResourceGroupData, DeviceProviderKind> allowedProviderDriver;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceGroupDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        descriptionDriver = new DescriptionDriver();
        layerStackDriver = new GenericStringListDriver<>(
            "layer stack",
            rscGrp -> rscGrp.getLayerStack(accCtx),
            UPDATE_LAYER_STACK
        );
        replicaCountDriver = new ReplicaCountDriver();
        storPoolNameDriver = new StorPoolNameDriver();
        doNotPlaceWithRscListDriver = new GenericStringListDriver<>(
            "'doNotPlaceWithRscList'",
            rscGrp -> rscGrp.getAutoPlaceConfig().getDoNotPlaceWithRscList(dbCtx),
            UPDATE_AP_DO_NOT_PLACE_WITH_LIST
        );
        doNotPlaceWithRscRegexDriver = new DoNotPlaceWithRscRegexDriver();
        replicasOnSameListDriver = new GenericStringListDriver<>(
            "replicas on same",
            rscGrp -> rscGrp.getAutoPlaceConfig().getReplicasOnSameList(dbCtx),
            UPDATE_AP_REPLICAS_ON_SAME
        );
        replicasOnDifferentListDriver = new GenericStringListDriver<>(
            "replicas on different",
            rscGrp -> rscGrp.getAutoPlaceConfig().getReplicasOnDifferentList(dbCtx),
            UPDATE_AP_REPLICAS_ON_DIFFERENT
        );
        allowedProviderDriver = new GenericStringListDriver<>(
            "allowed provider list",
            rscGrp -> rscGrp.getLayerStack(accCtx),
            UPDATE_AP_ALLOWED_PROVIDER_LIST
        );
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void persist(ResourceGroupData rscGrp) throws DatabaseException
    {
        errorReporter.logTrace("Creating ResourceGroup %s", getId(rscGrp));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            AutoSelectorConfig autoPlaceConfig = rscGrp.getAutoPlaceConfig();

            stmt.setString(1, rscGrp.getUuid().toString());
            stmt.setString(2, rscGrp.getName().value);
            stmt.setString(3, rscGrp.getName().displayValue);
            SQLUtils.setStringIfNotNull(stmt, 4, rscGrp.getDescription(dbCtx));
            SQLUtils.setJsonIfNotNull(stmt, 5, rscGrp.getLayerStack(dbCtx));
            SQLUtils.setIntIfNotNull(stmt, 6, autoPlaceConfig.getReplicaCount(dbCtx));
            SQLUtils.setStringIfNotNull(stmt, 7, autoPlaceConfig.getStorPoolNameStr(dbCtx));
            SQLUtils.setStringIfNotNull(stmt, 8, autoPlaceConfig.getDoNotPlaceWithRscRegex(dbCtx));
            SQLUtils.setJsonIfNotNull(stmt, 9, autoPlaceConfig.getDoNotPlaceWithRscList(dbCtx));
            SQLUtils.setJsonIfNotNull(stmt, 10, autoPlaceConfig.getReplicasOnSameList(dbCtx));
            SQLUtils.setJsonIfNotNull(stmt, 11, autoPlaceConfig.getReplicasOnDifferentList(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace("ResourceGroup created %s", getId(rscGrp));
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

    public Map<ResourceGroupData, InitMaps> loadAll() throws DatabaseException
    {
        errorReporter.logTrace("Loading all ResourceGroups");
        Map<ResourceGroupData, InitMaps> rscGrpMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RSC_GRPS))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Pair<ResourceGroupData, InitMaps> pair = restoreResourceGroup(resultSet);
                    rscGrpMap.put(pair.objA, pair.objB);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d ResourceGroup", rscGrpMap.size());
        return rscGrpMap;
    }

    private Pair<ResourceGroupData, InitMaps> restoreResourceGroup(ResultSet resultSet) throws DatabaseException
    {
        Pair<ResourceGroupData, InitMaps> retPair = new Pair<>();
        ResourceGroupData resGrp;
        ResourceGroupName rscGrpName;
        try
        {
            try
            {
                rscGrpName = new ResourceGroupName(resultSet.getString(RESOURCE_GROUP_DSP_NAME));
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The display name of a stored ResourceGroup in the table %s could not be restored. " +
                            "(invalid display RscGrpName=%s)",
                        TBL_RESOURCE_GROUPS,
                        resultSet.getString(RESOURCE_GROUP_DSP_NAME)
                    ),
                    invalidNameExc
                );
            }

            ObjectProtection objProt = getObjectProtection(rscGrpName);

            Map<VolumeNumber, VolumeGroup> vlmGrpMap = new TreeMap<>();
            Map<ResourceName, ResourceDefinition> rscDfnMap = new TreeMap<>();

            resGrp = new ResourceGroupData(
                java.util.UUID.fromString(resultSet.getString(UUID)),
                objProt,
                rscGrpName,
                resultSet.getString(DESCRIPTION),
                SQLUtils.getAsTypedList(resultSet, LAYER_KIND_STACK, DeviceLayerKind::valueOf),
                resultSet.getInt(REPLICA_COUNT),
                resultSet.getString(POOL_NAME),
                SQLUtils.getAsStringList(resultSet, DO_NOT_PLACE_WITH_RSC_LIST),
                resultSet.getString(DO_NOT_PLACE_WITH_RSC_REGEX),
                SQLUtils.getAsStringList(resultSet, REPLICAS_ON_SAME),
                SQLUtils.getAsStringList(resultSet, REPLICAS_ON_DIFFERENT),
                SQLUtils.getAsTypedList(resultSet, ALLOWED_PROVIDER_LIST, DeviceProviderKind::valueOf),
                vlmGrpMap,
                rscDfnMap,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );

            retPair.objA = resGrp;
            retPair.objB = new RscGrpInitMaps(rscDfnMap, vlmGrpMap);

            errorReporter.logTrace("ResourceGroup instance created %s", getId(resGrp));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return retPair;
    }

    private ObjectProtection getObjectProtection(ResourceGroupName rscGrpName)
        throws DatabaseException, ImplementationError
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(rscGrpName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "ResourceGroup's DB entry exists, but is missing an entry in ObjProt table! " +
                getId(rscGrpName), null
            );
        }
        return objProt;
    }

    @Override
    public void delete(ResourceGroupData rscGrp) throws DatabaseException
    {
        errorReporter.logTrace("Deleting ResourceGroup %s", getId(rscGrp));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
        {
            stmt.setString(1, rscGrp.getName().value);
            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("ResourceGroup deleted %s", getId(rscGrp));
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, String> getDescriptionDriver()
    {
        return descriptionDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroupData, DeviceLayerKind> getLayerStackDriver()
    {
        return layerStackDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, Integer> getReplicaCountDriver()
    {
        return replicaCountDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, String> getStorPoolNameDriver()
    {
        return storPoolNameDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroupData, String> getDoNotPlaceWithRscListDriver()
    {
        return doNotPlaceWithRscListDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, String> getDoNotPlaceWithRscRegexDriver()
    {
        return doNotPlaceWithRscRegexDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroupData, String> getReplicasOnDifferentDriver()
    {
        return replicasOnDifferentListDriver;
    }
    @Override
    public CollectionDatabaseDriver<ResourceGroupData, String> getReplicasOnSameListDriver()
    {
        return replicasOnSameListDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroupData, DeviceProviderKind> getAllowedProviderListDriver()
    {
        return allowedProviderDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(ResourceGroupData rscGrp)
    {
        return getId(rscGrp.getName().displayValue);
    }

    private String getId(ResourceGroupName rscGrpName)
    {
        return getId(rscGrpName.displayValue);
    }

    private String getId(String resGrpName)
    {
        return "(RscGrpName=" + resGrpName + ")";
    }

    private class DescriptionDriver implements SingleColumnDatabaseDriver<ResourceGroupData, String>
    {
        @Override
        public void update(ResourceGroupData rscGrp, String description)
            throws DatabaseException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceGroup's description from [%s] to [%s] %s",
                    rscGrp.getDescription(dbCtx),
                    description,
                    getId(rscGrp)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_DESCR))
                {
                    stmt.setString(1, description);
                    stmt.setString(2, rscGrp.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceGroup's description updated from [%s] to [%s] %s",
                    rscGrp.getDescription(dbCtx),
                    description,
                    getId(rscGrp)
                );
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
    }

    private class ReplicaCountDriver implements SingleColumnDatabaseDriver<ResourceGroupData, Integer>
    {
        @Override
        public void update(ResourceGroupData rscGrp, Integer replicaCount)
            throws DatabaseException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceGroup's replica count from [%d] to [%d] %s",
                    rscGrp.getAutoPlaceConfig().getReplicaCount(dbCtx),
                    replicaCount,
                    getId(rscGrp)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_AP_REPLICA_COUNT))
                {
                    stmt.setInt(1, replicaCount);
                    stmt.setString(2, rscGrp.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceGroup's replica count updated from [%d] to [%d] %s",
                    rscGrp.getAutoPlaceConfig().getReplicaCount(dbCtx),
                    replicaCount,
                    getId(rscGrp)
                );
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
    }

    private class StorPoolNameDriver implements SingleColumnDatabaseDriver<ResourceGroupData, String>
    {
        @Override
        public void update(ResourceGroupData rscGrp, String storPoolName)
            throws DatabaseException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceGroup's storage pool name from [%s] to [%s] %s",
                    rscGrp.getAutoPlaceConfig().getStorPoolNameStr(dbCtx),
                    getId(rscGrp)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_AP_STOR_POOL_NAME))
                {
                    stmt.setString(1, storPoolName);
                    stmt.setString(2, rscGrp.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceGroup's storage pool name updated from [%s] to [%s] %s",
                    rscGrp.getAutoPlaceConfig().getStorPoolNameStr(dbCtx),
                    storPoolName,
                    getId(rscGrp)
                );
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
    }

    private class DoNotPlaceWithRscRegexDriver implements SingleColumnDatabaseDriver<ResourceGroupData, String>
    {
        @Override
        public void update(ResourceGroupData rscGrp, String doNotPlaceWithRscRegex)
            throws DatabaseException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceGroup's 'doNotPlaceWithRscRegex' from [%s] to [%s] %s",
                    rscGrp.getAutoPlaceConfig().getDoNotPlaceWithRscRegex(dbCtx),
                    doNotPlaceWithRscRegex,
                    getId(rscGrp)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_AP_DO_NOT_PLACE_WITH_REGEX))
                {
                    stmt.setString(1, doNotPlaceWithRscRegex);
                    stmt.setString(2, rscGrp.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceGroup's 'doNotPlaceWithRscRegex' updated from [%s] to [%s] %s",
                    rscGrp.getAutoPlaceConfig().getDoNotPlaceWithRscRegex(dbCtx),
                    doNotPlaceWithRscRegex,
                    getId(rscGrp)
                );
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
    }

    private class GenericStringListDriver<TYPE> implements
        CollectionDatabaseDriver<ResourceGroupData, TYPE>
    {
        private final String columnDescription;
        private final ExceptionThrowingFunction<ResourceGroupData, Object, AccessDeniedException> getOldValueFkt;
        private final String updateStmt;

        GenericStringListDriver(
            String columnDescriptionRef,
            ExceptionThrowingFunction<ResourceGroupData, Object, AccessDeniedException> getOldValueFktRef,
            String updateStmtRef
        )
        {
            columnDescription = columnDescriptionRef;
            getOldValueFkt = getOldValueFktRef;
            updateStmt = updateStmtRef;
        }

        @Override
        public void insert(ResourceGroupData rscGrp, TYPE valueRef, Collection<TYPE> backingCollection)
            throws DatabaseException
        {
            update(rscGrp, backingCollection);
        }

        @Override
        public void remove(ResourceGroupData rscGrp, TYPE valueRef, Collection<TYPE> backingCollection)
            throws DatabaseException
        {
            update(rscGrp, backingCollection);
        }

        private void update(ResourceGroupData rscGrp, Collection<TYPE> backingCollection) throws DatabaseException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceGroup's %s from [%s] to [%s] %s",
                    columnDescription,
                    getOldValueFkt.accept(rscGrp).toString(),
                    backingCollection.toString(),
                    getId(rscGrp)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(updateStmt))
                {
                    SQLUtils.setJsonIfNotNull(stmt, 1, backingCollection);
                    stmt.setString(2, rscGrp.getName().value);
                    stmt.executeUpdate();
                }
                catch (SQLException sqlExc)
                {
                    throw new DatabaseException(sqlExc);
                }
                errorReporter.logTrace(
                    "ResourceGroup's %s updated from [%s] to [%s] %s",
                    columnDescription,
                    getOldValueFkt.accept(rscGrp).toString(),
                    backingCollection.toString(),
                    getId(rscGrp)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accDeniedExc);
            }
        }
    }


    private class RscGrpInitMaps implements ResourceGroup.InitMaps
    {
        private final Map<ResourceName, ResourceDefinition> rscDfnMap;
        private final Map<VolumeNumber, VolumeGroup> vlmGrpMap;

        private RscGrpInitMaps(
            Map<ResourceName, ResourceDefinition> rscDfnMapRef,
            Map<VolumeNumber, VolumeGroup> vlmGrpMapRef
        )
        {
            rscDfnMap = rscDfnMapRef;
            vlmGrpMap = vlmGrpMapRef;
        }

        @Override
        public Map<ResourceName, ResourceDefinition> getRscDfnMap()
        {
            return rscDfnMap;
        }

        @Override
        public Map<VolumeNumber, VolumeGroup> getVlmGrpMap()
        {
            return vlmGrpMap;
        }
    }
}
