package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
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

@Singleton
@Deprecated
public class SnapshotDefinitionGenericDbDriver implements SnapshotDefinitionDatabaseDriver
{
    private static final String TBL_SNAPSHOT_DFN = DbConstants.TBL_SNAPSHOT_DEFINITIONS;
    private static final String SD_UUID = DbConstants.UUID;
    private static final String SD_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String SD_NAME = DbConstants.SNAPSHOT_NAME;
    private static final String SD_DSP_NAME = DbConstants.SNAPSHOT_DSP_NAME;
    private static final String SD_FLAGS = DbConstants.SNAPSHOT_FLAGS;
    private static final String[] SD_FIELDS = {
        SD_UUID,
        SD_RES_NAME,
        SD_NAME,
        SD_DSP_NAME,
        SD_FLAGS
    };

    private static final String SD_SELECT_ALL =
        " SELECT " + StringUtils.join(", ", SD_FIELDS) +
        " FROM " + TBL_SNAPSHOT_DFN;

    private static final String SD_INSERT =
        " INSERT INTO " + TBL_SNAPSHOT_DFN +
        " (" + StringUtils.join(", ", SD_FIELDS) + ")" +
        " VALUES (" + StringUtils.repeat("?", ", ", SD_FIELDS.length) + ")";

    private static final String SD_UPDATE_FLAGS =
        " UPDATE " + TBL_SNAPSHOT_DFN +
        " SET " + SD_FLAGS + " = ? " +
        " WHERE " + SD_RES_NAME + " = ? AND " +
            SD_NAME + " = ?";

    private static final String SD_DELETE =
        " DELETE FROM " + TBL_SNAPSHOT_DFN +
        " WHERE " + SD_RES_NAME + " = ? AND " +
            SD_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final PropsContainerFactory propsContainerFactory;

    private final FlagDriver flagsDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final ObjectProtectionDatabaseDriver objProtDriver;

    @Inject
    public SnapshotDefinitionGenericDbDriver(
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
        flagsDriver = new FlagDriver();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(SnapshotDefinition snapshotDefinition) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(SD_INSERT))
        {
            errorReporter.logTrace("Creating SnapshotDefinition %s", getId(snapshotDefinition));

            stmt.setString(1, snapshotDefinition.getUuid().toString());
            stmt.setString(2, snapshotDefinition.getResourceName().value);
            stmt.setString(3, snapshotDefinition.getName().value);
            stmt.setString(4, snapshotDefinition.getName().displayValue);
            stmt.setLong(5, snapshotDefinition.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace("SnapshotDefinition created %s", getId(snapshotDefinition));
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

    private Pair<SnapshotDefinition, SnapshotDefinition.InitMaps> restoreSnapshotDefinition(
        ResultSet resultSet,
        ResourceDefinition resDfn,
        SnapshotName snapshotName
    )
        throws DatabaseException
    {
        errorReporter.logTrace("Restoring SnapshotDefinition %s", getId(resDfn, snapshotName));
        SnapshotDefinition snapshotDfn;
        Pair<SnapshotDefinition, SnapshotDefinition.InitMaps> retPair;

        Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVlmDfnMap = new TreeMap<>();
        Map<NodeName, Snapshot> snapshotMap = new TreeMap<>();

        try {
            snapshotDfn = new SnapshotDefinition(
                java.util.UUID.fromString(resultSet.getString(SD_UUID)),
                getObjectProtection(resDfn.getName(), snapshotName),
                resDfn,
                snapshotName,
                resultSet.getLong(SD_FLAGS),
                this,
                transObjFactory,
                propsContainerFactory,
                transMgrProvider,
                snapshotVlmDfnMap,
                snapshotMap,
                new TreeMap<>()
            );
            retPair = new Pair<>(snapshotDfn, new SnapshotDefinitionInitMaps(snapshotMap, snapshotVlmDfnMap));

            errorReporter.logTrace("SnapshotDefinition %s created during restore", getId(snapshotDfn));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        return retPair;
    }


    private ObjectProtection getObjectProtection(ResourceName resourceName, SnapshotName snapName)
        throws DatabaseException, ImplementationError
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(resourceName, snapName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "ResourceDefinition's DB entry exists, but is missing an entry in ObjProt table! " +
                    getId(resourceName.displayValue, snapName),
                null
            );
        }
        return objProt;
    }

    public Map<SnapshotDefinition, SnapshotDefinition.InitMaps> loadAll(
        Map<ResourceName, ? extends ResourceDefinition> rscDfnMap
    )
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all SnapshotDefinitions");
        Map<SnapshotDefinition, SnapshotDefinition.InitMaps> ret = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    ResourceName rscName;
                    SnapshotName snapshotName;
                    try
                    {
                        rscName = new ResourceName(resultSet.getString(SD_RES_NAME));
                        snapshotName = new SnapshotName(resultSet.getString(SD_DSP_NAME));
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            TBL_SNAPSHOT_DFN + " contained invalid name: " + exc.invalidName,
                            exc
                        );
                    }

                    Pair<SnapshotDefinition, SnapshotDefinition.InitMaps> pair = restoreSnapshotDefinition(
                        resultSet,
                        rscDfnMap.get(rscName),
                        snapshotName
                    );

                    ret.put(pair.objA, pair.objB);

                    errorReporter.logTrace("SnapshotDefinition created %s", getId(pair.objA));
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return ret;
    }

    @Override
    public void delete(SnapshotDefinition snapshotDefinition) throws DatabaseException
    {
        errorReporter.logTrace("Deleting SnapshotDefinition %s", getId(snapshotDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(SD_DELETE))
        {
            stmt.setString(1, snapshotDefinition.getResourceName().value);
            stmt.setString(2, snapshotDefinition.getName().value);
            stmt.executeUpdate();
            errorReporter.logTrace("SnapshotDefinition deleted %s", getId(snapshotDefinition));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public StateFlagsPersistence<SnapshotDefinition> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(SnapshotDefinition snapshotDfn)
    {
        return getId(
            snapshotDfn.getResourceDefinition(),
            snapshotDfn.getName()
        );
    }

    private String getId(ResourceDefinition resourceDefinition, SnapshotName snapshotName)
    {
        return getId(
            resourceDefinition.getName().displayValue,
            snapshotName
        );
    }

    private String getId(String resName, SnapshotName snapshotName)
    {
        return "(ResName=" + resName + " SnapshotName=" + snapshotName.displayValue + ")";
    }

    private class FlagDriver implements StateFlagsPersistence<SnapshotDefinition>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(SnapshotDefinition snapshotDefinition, long flags)
            throws DatabaseException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(SD_UPDATE_FLAGS))
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        SnapshotDefinition.Flags.class,
                        snapshotDefinition.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        SnapshotDefinition.Flags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating SnapshotDefinition's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(snapshotDefinition)
                );
                stmt.setLong(1, flags);
                stmt.setString(2, snapshotDefinition.getResourceName().value);
                stmt.setString(3, snapshotDefinition.getName().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "SnapshotDefinition's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(snapshotDefinition)
                );
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
    }

    private class SnapshotDefinitionInitMaps implements SnapshotDefinition.InitMaps
    {
        private final Map<NodeName, Snapshot> snapshotMap;

        private final Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVolumeDefinitionMap;

        SnapshotDefinitionInitMaps(
            Map<NodeName, Snapshot> snapshotMapRef,
            Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVolumeDefinitionMapRef
        )
        {
            snapshotMap = snapshotMapRef;
            snapshotVolumeDefinitionMap = snapshotVolumeDefinitionMapRef;
        }

        @Override
        public Map<NodeName, Snapshot> getSnapshotMap()
        {
            return snapshotMap;
        }

        @Override
        public Map<VolumeNumber, SnapshotVolumeDefinition> getSnapshotVolumeDefinitionMap()
        {
            return snapshotVolumeDefinitionMap;
        }
    }
}
