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
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.ALLOWED_PROVIDER_LIST;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DESCRIPTION;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DISKLESS_ON_REMAINING;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DO_NOT_PLACE_WITH_RSC_LIST;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DO_NOT_PLACE_WITH_RSC_REGEX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_STACK;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.POOL_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.REPLICAS_ON_DIFFERENT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.REPLICAS_ON_SAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.REPLICA_COUNT;
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
import java.sql.Types;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class ResourceGroupGenericDbDriver implements ResourceGroupDatabaseDriver
{
    private static final String[] RSC_GRP_FIELDS =
    {
        UUID,
        RESOURCE_GROUP_NAME,
        RESOURCE_GROUP_DSP_NAME,
        DESCRIPTION,
        LAYER_STACK,
        ALLOWED_PROVIDER_LIST,
        REPLICA_COUNT,
        POOL_NAME,
        DO_NOT_PLACE_WITH_RSC_REGEX,
        DO_NOT_PLACE_WITH_RSC_LIST,
        REPLICAS_ON_SAME,
        REPLICAS_ON_DIFFERENT,
        DISKLESS_ON_REMAINING
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
        " SET " + LAYER_STACK + " = ? " +
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
    private static final String UPDATE_AP_DISKLESS_ON_REMAINING =
        " UPDATE " + TBL_RESOURCE_GROUPS +
        " SET " + DISKLESS_ON_REMAINING + " = ? " +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";

    private static final String DELETE =
        " DELETE FROM " + TBL_RESOURCE_GROUPS +
        " WHERE " + RESOURCE_GROUP_NAME + " = ?";


    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final SingleColumnDatabaseDriver<ResourceGroup, String> descriptionDriver;
    private final CollectionDatabaseDriver<ResourceGroup, DeviceLayerKind> layerStackDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, Integer> replicaCountDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, String>  storPoolNameDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> doNotPlaceWithRscListDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, String> doNotPlaceWithRscRegexDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> replicasOnSameListDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> replicasOnDifferentListDriver;
    private final CollectionDatabaseDriver<ResourceGroup, DeviceProviderKind> allowedProviderDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, Boolean> disklessOnRemainingDriver;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public ResourceGroupGenericDbDriver(
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

        descriptionDriver = new DescriptionDriver();
        layerStackDriver = new GenericStringListDriver<>(
            "layer stack",
            rscGrp -> rscGrp.getAutoPlaceConfig().getLayerStackList(accCtx),
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
            rscGrp -> rscGrp.getAutoPlaceConfig().getProviderList(accCtx),
            UPDATE_AP_ALLOWED_PROVIDER_LIST
        );
        disklessOnRemainingDriver = new DisklessOnRemainingDriver();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(ResourceGroup rscGrp) throws DatabaseException
    {
        errorReporter.logTrace("Creating ResourceGroup %s", getId(rscGrp));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            AutoSelectorConfig autoPlaceConfig = rscGrp.getAutoPlaceConfig();

            stmt.setString(1, rscGrp.getUuid().toString());
            stmt.setString(2, rscGrp.getName().value);
            stmt.setString(3, rscGrp.getName().displayValue);
            SQLUtils.setStringIfNotNull(stmt, 4, rscGrp.getDescription(dbCtx));
            SQLUtils.setJsonIfNotNullAsVarchar(stmt, 5, autoPlaceConfig.getLayerStackList(dbCtx));
            SQLUtils.setJsonIfNotNullAsVarchar(stmt, 6, autoPlaceConfig.getProviderList(dbCtx));
            SQLUtils.setIntIfNotNull(stmt, 7, autoPlaceConfig.getReplicaCount(dbCtx));
            SQLUtils.setStringIfNotNull(stmt, 8, autoPlaceConfig.getStorPoolNameStr(dbCtx));
            SQLUtils.setStringIfNotNull(stmt, 9, autoPlaceConfig.getDoNotPlaceWithRscRegex(dbCtx));
            SQLUtils.setJsonIfNotNullAsVarchar(stmt, 10, autoPlaceConfig.getDoNotPlaceWithRscList(dbCtx));
            SQLUtils.setJsonIfNotNullAsBlob(stmt, 11, autoPlaceConfig.getReplicasOnSameList(dbCtx));
            SQLUtils.setJsonIfNotNullAsBlob(stmt, 12, autoPlaceConfig.getReplicasOnDifferentList(dbCtx));
            SQLUtils.setBooleanIfNotNull(stmt, 13, autoPlaceConfig.getDisklessOnRemaining(dbCtx));

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

    public Map<ResourceGroup, ResourceGroup.InitMaps> loadAll() throws DatabaseException
    {
        errorReporter.logTrace("Loading all ResourceGroups");
        Map<ResourceGroup, ResourceGroup.InitMaps> rscGrpMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RSC_GRPS))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Pair<ResourceGroup, ResourceGroup.InitMaps> pair = restoreResourceGroup(resultSet);
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

    private Pair<ResourceGroup, ResourceGroup.InitMaps> restoreResourceGroup(ResultSet resultSet) throws DatabaseException
    {
        Pair<ResourceGroup, ResourceGroup.InitMaps> retPair = new Pair<>();
        ResourceGroup resGrp;
        ResourceGroupName rscGrpName;
        try
        {
            String rscGrpNameStr = null;
            try
            {
                rscGrpNameStr = resultSet.getString(RESOURCE_GROUP_DSP_NAME);
                rscGrpName = new ResourceGroupName(rscGrpNameStr);
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The display name of a stored ResourceGroup in the table %s could not be restored. " +
                            "(invalid display RscGrpName=%s)",
                        TBL_RESOURCE_GROUPS,
                        rscGrpNameStr
                    ),
                    invalidNameExc
                );
            }

            ObjectProtection objProt = getObjectProtection(rscGrpName);

            Map<VolumeNumber, VolumeGroup> vlmGrpMap = new TreeMap<>();
            Map<ResourceName, ResourceDefinition> rscDfnMap = new TreeMap<>();

            resGrp = new ResourceGroup(
                java.util.UUID.fromString(resultSet.getString(UUID)),
                objProt,
                rscGrpName,
                resultSet.getString(DESCRIPTION),
                SQLUtils.getAsTypedList(resultSet, LAYER_STACK, DeviceLayerKind::valueOf),
                SQLUtils.getNullableInteger(resultSet, REPLICA_COUNT),
                resultSet.getString(POOL_NAME),
                SQLUtils.getAsStringListFromVarchar(resultSet, DO_NOT_PLACE_WITH_RSC_LIST),
                resultSet.getString(DO_NOT_PLACE_WITH_RSC_REGEX),
                SQLUtils.getAsStringListFromBlob(resultSet, REPLICAS_ON_SAME),
                SQLUtils.getAsStringListFromBlob(resultSet, REPLICAS_ON_DIFFERENT),
                SQLUtils.getAsTypedList(resultSet, ALLOWED_PROVIDER_LIST, DeviceProviderKind::valueOf),
                SQLUtils.getNullableBoolean(resultSet, DISKLESS_ON_REMAINING),
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
    public void delete(ResourceGroup rscGrp) throws DatabaseException
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
    public SingleColumnDatabaseDriver<ResourceGroup, String> getDescriptionDriver()
    {
        return descriptionDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, DeviceLayerKind> getLayerStackDriver()
    {
        return layerStackDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, Integer> getReplicaCountDriver()
    {
        return replicaCountDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, String> getStorPoolNameDriver()
    {
        return storPoolNameDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscListDriver()
    {
        return doNotPlaceWithRscListDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscRegexDriver()
    {
        return doNotPlaceWithRscRegexDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnDifferentDriver()
    {
        return replicasOnDifferentListDriver;
    }
    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnSameListDriver()
    {
        return replicasOnSameListDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, DeviceProviderKind> getAllowedProviderListDriver()
    {
        return allowedProviderDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, Boolean> getDisklessOnRemainingDriver()
    {
        return disklessOnRemainingDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(ResourceGroup rscGrp)
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

    private class DescriptionDriver implements SingleColumnDatabaseDriver<ResourceGroup, String>
    {
        @Override
        public void update(ResourceGroup rscGrp, String description)
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

    private class ReplicaCountDriver implements SingleColumnDatabaseDriver<ResourceGroup, Integer>
    {
        @Override
        public void update(ResourceGroup rscGrp, Integer replicaCount)
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

    private class StorPoolNameDriver implements SingleColumnDatabaseDriver<ResourceGroup, String>
    {
        @Override
        public void update(ResourceGroup rscGrp, String storPoolName)
            throws DatabaseException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceGroup's storage pool name from [%s] to [%s] %s",
                    rscGrp.getAutoPlaceConfig().getStorPoolNameStr(dbCtx),
                    storPoolName,
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

    private class DoNotPlaceWithRscRegexDriver implements SingleColumnDatabaseDriver<ResourceGroup, String>
    {
        @Override
        public void update(ResourceGroup rscGrp, String doNotPlaceWithRscRegex)
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

    private class DisklessOnRemainingDriver implements SingleColumnDatabaseDriver<ResourceGroup, Boolean>
    {
        @Override
        public void update(ResourceGroup rscGrp, Boolean disklessOnRemaining)
            throws DatabaseException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceGroup's 'disklessOnRemaining' from [%s] to [%s] %s",
                    rscGrp.getAutoPlaceConfig().getDisklessOnRemaining(dbCtx),
                    disklessOnRemaining,
                    getId(rscGrp)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_AP_DISKLESS_ON_REMAINING))
                {
                    if (disklessOnRemaining == null)
                    {
                        stmt.setNull(1, Types.BOOLEAN);
                    }
                    else
                    {
                        stmt.setBoolean(1, disklessOnRemaining);
                    }
                    stmt.setString(2, rscGrp.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceGroup's 'disklessOnRemaining' updated from [%s] to [%s] %s",
                    rscGrp.getAutoPlaceConfig().getDisklessOnRemaining(dbCtx),
                    disklessOnRemaining,
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
        CollectionDatabaseDriver<ResourceGroup, TYPE>
    {
        private final String columnDescription;
        private final ExceptionThrowingFunction<ResourceGroup, Object, AccessDeniedException> getOldValueFkt;
        private final String updateStmt;

        GenericStringListDriver(
            String columnDescriptionRef,
            ExceptionThrowingFunction<ResourceGroup, Object, AccessDeniedException> getOldValueFktRef,
            String updateStmtRef
        )
        {
            columnDescription = columnDescriptionRef;
            getOldValueFkt = getOldValueFktRef;
            updateStmt = updateStmtRef;
        }

        @Override
        public void insert(ResourceGroup rscGrp, TYPE valueRef, Collection<TYPE> backingCollection)
            throws DatabaseException
        {
            update(rscGrp, backingCollection);
        }

        @Override
        public void remove(ResourceGroup rscGrp, TYPE valueRef, Collection<TYPE> backingCollection)
            throws DatabaseException
        {
            update(rscGrp, backingCollection);
        }

        private void update(ResourceGroup rscGrp, Collection<TYPE> backingCollection) throws DatabaseException
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
                    SQLUtils.setJsonIfNotNullAsVarchar(stmt, 1, backingCollection);
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
