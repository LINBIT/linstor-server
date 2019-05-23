package com.linbit.linstor.core.objects;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.ResourceDefinition.InitMaps;
import com.linbit.linstor.core.objects.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_STACK;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_EXTERNAL_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_GROUP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_RESOURCE_DEFINITIONS;
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
public class ResourceDefinitionDataGenericDbDriver implements ResourceDefinitionDataDatabaseDriver
{
    private static final String TBL_RES_DEF = TBL_RESOURCE_DEFINITIONS;
    private static final String RD_UUID = UUID;
    private static final String RD_NAME = RESOURCE_NAME;
    private static final String RD_DSP_NAME = RESOURCE_DSP_NAME;
    private static final String RD_FLAGS = RESOURCE_FLAGS;
    private static final String RD_LAYERS = LAYER_STACK;
    private static final String RD_EXT_NAME = RESOURCE_EXTERNAL_NAME;
    private static final String RD_RSC_GRP_NAME = RESOURCE_GROUP_NAME;

    private static final String[] RSC_DFN_FIELDS = {
        RD_UUID,
        RD_NAME,
        RD_DSP_NAME,
        RD_FLAGS,
        RD_LAYERS,
        RD_EXT_NAME,
        RD_RSC_GRP_NAME
    };

    private static final String RD_SELECT_ALL =
        " SELECT " + StringUtils.join(", ", RSC_DFN_FIELDS) +
        " FROM " + TBL_RES_DEF;

    private static final String RD_SELECT =
        RD_SELECT_ALL +
        " WHERE " + RD_NAME + " = ?";

    private static final String RD_INSERT =
        " INSERT INTO " + TBL_RES_DEF +
        " (" + StringUtils.join(", ", RSC_DFN_FIELDS) + ")" +
        " VALUES ( " + StringUtils.repeat("?", ", ", RSC_DFN_FIELDS.length) + " )";

    private static final String RD_UPDATE_FLAGS =
        " UPDATE " + TBL_RES_DEF +
        " SET " + RD_FLAGS + " = ? " +
        " WHERE " + RD_NAME + " = ?";

    private static final String UPDATE_LAYER_STACK =
       " UPDATE " + TBL_RES_DEF +
       " SET " + RD_LAYERS + " = ? " +
       " WHERE " + RD_NAME + " = ?";

    private static final String RD_DELETE =
        " DELETE FROM " + TBL_RES_DEF +
        " WHERE " + RD_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<ResourceDefinitionData> resDfnFlagPersistence;
    private final CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind> layerStackDriver;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDefinitionDataGenericDbDriver(
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
        resDfnFlagPersistence = new ResDfnFlagsPersistence();
        layerStackDriver = new LayerStackDriver();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(ResourceDefinitionData resourceDefinition) throws DatabaseException
    {
        errorReporter.logTrace("Creating ResourceDfinition %s", getId(resourceDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_INSERT))
        {
            stmt.setString(1, resourceDefinition.getUuid().toString());
            stmt.setString(2, resourceDefinition.getName().value);
            stmt.setString(3, resourceDefinition.getName().displayValue);
            stmt.setLong(4, resourceDefinition.getFlags().getFlagsBits(dbCtx));
            SQLUtils.setJsonIfNotNullAsVarchar(
                stmt,
                5,
                DatabaseLoader.asStrList(resourceDefinition.getLayerStack(dbCtx))
            );
            stmt.setBytes(6, resourceDefinition.getExternalName());
            stmt.setString(7, resourceDefinition.getResourceGroup().getName().value);
            stmt.executeUpdate();

            errorReporter.logTrace("ResourceDefinition created %s", getId(resourceDefinition));
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
    public boolean exists(ResourceName resourceName) throws DatabaseException
    {
        boolean exists;
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_SELECT))
        {
            stmt.setString(1, resourceName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                exists = resultSet.next();
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return exists;
    }

    public Map<ResourceDefinitionData, InitMaps> loadAll(Map<ResourceGroupName, ResourceGroup> rscGrpMapRef)
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all ResourceDefinitions");
        Map<ResourceDefinitionData, InitMaps> rscDfnMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Pair<ResourceDefinitionData, InitMaps> pair = restoreRscDfn(resultSet, rscGrpMapRef);
                    rscDfnMap.put(pair.objA, pair.objB);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d ResourceDefinitions", rscDfnMap.size());
        return rscDfnMap;
    }

    private Pair<ResourceDefinitionData, InitMaps> restoreRscDfn(
        ResultSet resultSet,
        Map<ResourceGroupName, ResourceGroup> rscGrpMapRef
    )
        throws DatabaseException
    {
        Pair<ResourceDefinitionData, InitMaps> retPair = new Pair<>();
        ResourceDefinitionData resDfn;
        ResourceName resourceName;
        try
        {
            try
            {
                resourceName = new ResourceName(resultSet.getString(RD_DSP_NAME));
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The display name of a stored ResourceDefinition in the table %s could not be restored. " +
                            "(invalid display ResName=%s)",
                        TBL_RES_DEF,
                        resultSet.getString(RD_DSP_NAME)
                    ),
                    invalidNameExc
                );
            }

            ObjectProtection objProt = getObjectProtection(resourceName);

            Map<VolumeNumber, VolumeDefinition> vlmDfnMap = new TreeMap<>();
            Map<NodeName, Resource> rscMap = new TreeMap<>();
            Map<SnapshotName, SnapshotDefinition> snapshotDfnMap = new TreeMap<>();

            ResourceGroup rscGrp;
            try
            {
                rscGrp = rscGrpMapRef.get(new ResourceGroupName(resultSet.getString(RD_RSC_GRP_NAME)));
            }
            catch (InvalidNameException exc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The resource group name of a stored ResourceDefinition in the table %s could not be restored. " +
                            "(resource group name=%s)",
                        TBL_RES_DEF,
                        resultSet.getString(RD_RSC_GRP_NAME)
                    )
                );
            }
            resDfn = new ResourceDefinitionData(
                java.util.UUID.fromString(resultSet.getString(RD_UUID)),
                objProt,
                resourceName,
                resultSet.getBytes(RD_EXT_NAME),
                resultSet.getLong(RD_FLAGS),
                DatabaseLoader.asDevLayerKindList(SQLUtils.getAsStringListFromVarchar(resultSet, RD_LAYERS)),
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                vlmDfnMap,
                rscMap,
                snapshotDfnMap,
                new TreeMap<>(),
                rscGrp
            );

            retPair.objA = resDfn;
            retPair.objB = new RscDfnInitMaps(vlmDfnMap, rscMap, snapshotDfnMap);

            errorReporter.logTrace("ResourceDefinition instance created %s", getId(resDfn));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return retPair;
    }

    private ObjectProtection getObjectProtection(ResourceName resourceName)
        throws DatabaseException, ImplementationError
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(resourceName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "ResourceDefinition's DB entry exists, but is missing an entry in ObjProt table! " +
                getId(resourceName), null
            );
        }
        return objProt;
    }

    @Override
    public void delete(ResourceDefinitionData resourceDefinition) throws DatabaseException
    {
        errorReporter.logTrace("Deleting ResourceDefinition %s", getId(resourceDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_DELETE))
        {
            stmt.setString(1, resourceDefinition.getName().value);
            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("ResourceDfinition deleted %s", getId(resourceDefinition));
    }

    @Override
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return resDfnFlagPersistence;
    }

    @Override
    public CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind> getLayerStackDriver()
    {
        return layerStackDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(ResourceDefinitionData resourceDefinition)
    {
        return getId(resourceDefinition.getName().displayValue);
    }

    private String getId(ResourceName resourceName)
    {
        return getId(resourceName.displayValue);
    }

    private String getId(String resName)
    {
        return "(ResName=" + resName + ")";
    }

    private class ResDfnFlagsPersistence implements StateFlagsPersistence<ResourceDefinitionData>
    {
        @Override
        public void persist(ResourceDefinitionData resourceDefinition, long flags)
            throws DatabaseException
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        RscDfnFlags.class,
                        resourceDefinition.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        RscDfnFlags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating ResourceDefinition's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(resourceDefinition)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(RD_UPDATE_FLAGS))
                {
                    stmt.setLong(1, flags);
                    stmt.setString(2, resourceDefinition.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceDefinition's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(resourceDefinition)
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

    private class LayerStackDriver implements CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind>
    {
        @Override
        public void insert(
            ResourceDefinitionData rscDfn,
            DeviceLayerKind newElem,
            Collection<DeviceLayerKind> backingCollection
        )
            throws DatabaseException
        {
            update(rscDfn, backingCollection);
        }

        @Override
        public void remove(
            ResourceDefinitionData rscDfn,
            DeviceLayerKind newElem,
            Collection<DeviceLayerKind> backingCollection
        )
            throws DatabaseException
        {
            update(rscDfn, backingCollection);
        }

        public void update(ResourceDefinitionData rscDfn, Collection<DeviceLayerKind> backingCollection)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating ResourceDefinition's layer stack to %s %s",
                backingCollection.toString(),
                getId(rscDfn)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_LAYER_STACK))
            {
                SQLUtils.setJsonIfNotNullAsVarchar(stmt, 1, DatabaseLoader.asStrList(backingCollection));
                stmt.setString(2, rscDfn.getName().value);
                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "ResourceDefinition's layer stack updated to %s %s",
                backingCollection.toString(),
                getId(rscDfn)
            );
        }
    }

    private class RscDfnInitMaps implements ResourceDefinition.InitMaps
    {
        private final Map<VolumeNumber, VolumeDefinition> vlmDfnMap;
        private final Map<NodeName, Resource> rscMap;
        private final Map<SnapshotName, SnapshotDefinition> snapshotDfnMap;

        RscDfnInitMaps(
            Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
            Map<NodeName, Resource> rscMapRef,
            Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef
        )
        {
            this.vlmDfnMap = vlmDfnMapRef;
            this.rscMap = rscMapRef;
            this.snapshotDfnMap = snapshotDfnMapRef;
        }

        @Override
        public Map<NodeName, Resource> getRscMap()
        {
            return rscMap;
        }

        @Override
        public Map<VolumeNumber, VolumeDefinition> getVlmDfnMap()
        {
            return vlmDfnMap;
        }

        @Override
        public Map<SnapshotName, SnapshotDefinition> getSnapshotDfnMap()
        {
            return snapshotDfnMap;
        }
    }
}
