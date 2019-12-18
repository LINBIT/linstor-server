package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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
public class SnapshotVolumeDefinitionGenericDbDriver implements SnapshotVolumeDefinitionDatabaseDriver
{
    private static final String TBL_SNAPSHOT_VOLUME_DEFINITIONS = DbConstants.TBL_SNAPSHOT_VOLUME_DEFINITIONS;

    private static final String SVD_UUID = DbConstants.UUID;
    private static final String SVD_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String SVD_SNAPSHOT_NAME = DbConstants.SNAPSHOT_NAME;
    private static final String SVD_VLM_NR = DbConstants.VLM_NR;
    private static final String SVD_SIZE = DbConstants.VLM_SIZE;
    private static final String SVD_FLAGS = DbConstants.SNAPSHOT_FLAGS;

    private static final String SVD_SELECT_ALL =
        " SELECT " + SVD_UUID + ", " + SVD_RES_NAME + ", " + SVD_SNAPSHOT_NAME + ", " + SVD_VLM_NR + ", " +
            SVD_SIZE + ", " + SVD_FLAGS +
        " FROM " + TBL_SNAPSHOT_VOLUME_DEFINITIONS;

    private static final String SVD_INSERT =
        " INSERT INTO " + TBL_SNAPSHOT_VOLUME_DEFINITIONS +
        " (" +
            SVD_UUID + ", " + SVD_RES_NAME + ", " + SVD_SNAPSHOT_NAME + ", " + SVD_VLM_NR + ", " +
            SVD_SIZE + ", " + SVD_FLAGS +
        ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String SVD_UPDATE_SIZE =
        " UPDATE " + TBL_SNAPSHOT_VOLUME_DEFINITIONS +
            " SET " + SVD_SIZE + " = ? " +
            " WHERE " + SVD_RES_NAME + " = ? AND " +
                SVD_SNAPSHOT_NAME + " = ? AND " +
                SVD_VLM_NR + " = ?";
    private static final String SVD_UPDATE_FLAGS =
        " UPDATE " + TBL_SNAPSHOT_VOLUME_DEFINITIONS +
            " SET " + SVD_FLAGS + " = ? " +
            " WHERE " + SVD_RES_NAME + " = ? AND " +
                SVD_SNAPSHOT_NAME + " = ? AND " +
                SVD_VLM_NR + " = ?";
    private static final String SVD_DELETE =
        " DELETE FROM " + TBL_SNAPSHOT_VOLUME_DEFINITIONS +
        " WHERE " + SVD_RES_NAME + " = ? AND " +
            SVD_SNAPSHOT_NAME + " = ? AND " +
            SVD_VLM_NR + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final PropsContainerFactory propsContainerFactory;

    private final SizeDriver sizeDriver;
    private final FlagDriver flagsDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public SnapshotVolumeDefinitionGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        sizeDriver = new SizeDriver();
        flagsDriver = new FlagDriver();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(SnapshotVolumeDefinition snapshotVolumeDefinition) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(SVD_INSERT))
        {
            errorReporter.logTrace("Creating SnapshotVolumeDefinition %s", getId(snapshotVolumeDefinition));

            stmt.setString(1, snapshotVolumeDefinition.getUuid().toString());
            stmt.setString(2, snapshotVolumeDefinition.getResourceName().value);
            stmt.setString(3, snapshotVolumeDefinition.getSnapshotName().value);
            stmt.setInt(4, snapshotVolumeDefinition.getVolumeNumber().value);
            stmt.setLong(5, snapshotVolumeDefinition.getVolumeSize(dbCtx));
            stmt.setLong(6, snapshotVolumeDefinition.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace("SnapshotVolumeDefinition created %s", getId(snapshotVolumeDefinition));
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


    private Pair<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> restoreSnapshotVolumeDefinition(
        ResultSet resultSet,
        SnapshotDefinition snapshotDefinition,
        VolumeDefinition vlmDfn
    )
        throws DatabaseException
    {
        errorReporter.logTrace(
            "Restoring SnapshotVolumeDefinition %s",
            getId(snapshotDefinition, vlmDfn.getVolumeNumber())
        );
        Pair<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> retPair;

        long volSize = -1;
        try
        {
            volSize = resultSet.getLong(SVD_SIZE);
            Map<NodeName, SnapshotVolume> snapshotVlmMap = new TreeMap<>();

            SnapshotVolumeDefinition snapshotVolumeDefinition = new SnapshotVolumeDefinition(
                java.util.UUID.fromString(resultSet.getString(SVD_UUID)),
                snapshotDefinition,
                vlmDfn,
                vlmDfn.getVolumeNumber(), // this class should be deprecated and no longer used
                volSize,
                resultSet.getLong(SVD_FLAGS),
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                snapshotVlmMap,
                new TreeMap<>()
            );

            retPair = new Pair<>(snapshotVolumeDefinition, new SnapshotVolumeDefinitionInitMaps(snapshotVlmMap));

            errorReporter.logTrace(
                "SnapshotVolumeDefinition %s created during restore", getId(snapshotVolumeDefinition));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (MdException mdExc)
        {
            throw new LinStorDBRuntimeException(
                String.format(
                    "A VolumeSize of a stored SnapshotVolumeDefinition in table %s could not be restored. " +
                        "(ResourceName=%s, SnapshotName=%s, VolumeNumber=%d, invalid VolumeSize=%d)",
                    TBL_SNAPSHOT_VOLUME_DEFINITIONS,
                    snapshotDefinition.getResourceName().value,
                    snapshotDefinition.getName().value,
                    vlmDfn.getVolumeNumber(),
                    volSize
                ),
                mdExc
            );
        }

        return retPair;
    }


    public Map<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> loadAll(
        Map<Pair<ResourceName, SnapshotName>, ? extends SnapshotDefinition> snapshotDfnMap,
        Map<Pair<ResourceName, VolumeNumber>, ? extends VolumeDefinition> vlmDfnMap
    )
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all SnapshotVolumeDefinitions");
        Map<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> loadedSnapshotVlmDfns = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SVD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    int vlmNr = resultSet.getInt(SVD_VLM_NR);

                    ResourceName rscName;
                    SnapshotName snapshotName;
                    VolumeNumber volumeNumber;
                    try
                    {
                        rscName = new ResourceName(resultSet.getString(SVD_RES_NAME));
                        snapshotName = new SnapshotName(resultSet.getString(SVD_SNAPSHOT_NAME));
                        volumeNumber = new VolumeNumber(vlmNr);
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            TBL_SNAPSHOT_VOLUME_DEFINITIONS + " contained invalid name: " + exc.invalidName,
                            exc
                        );
                    }
                    catch (ValueOutOfRangeException exc)
                    {
                        throw new ImplementationError(
                            TBL_SNAPSHOT_VOLUME_DEFINITIONS + " contained invalid volume number: " + vlmNr,
                            exc
                        );
                    }

                    Pair<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> pair =
                        restoreSnapshotVolumeDefinition(
                            resultSet,
                            snapshotDfnMap.get(new Pair<>(rscName, snapshotName)),
                            vlmDfnMap.get(new Pair<>(rscName, volumeNumber))
                        );

                    loadedSnapshotVlmDfns.put(pair.objA, pair.objB);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return loadedSnapshotVlmDfns;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(SnapshotVolumeDefinition snapshotVolumeDefinition) throws DatabaseException
    {
        errorReporter.logTrace("Deleting SnapshotVolumeDefinition %s", getId(snapshotVolumeDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(SVD_DELETE))
        {
            stmt.setString(1, snapshotVolumeDefinition.getResourceName().value);
            stmt.setString(2, snapshotVolumeDefinition.getSnapshotName().value);
            stmt.setInt(3, snapshotVolumeDefinition.getVolumeNumber().value);
            stmt.executeUpdate();
            errorReporter.logTrace("SnapshotVolumeDefinition deleted %s", getId(snapshotVolumeDefinition));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> getVolumeSizeDriver()
    {
        return sizeDriver;
    }

    @Override
    public StateFlagsPersistence<SnapshotVolumeDefinition> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(SnapshotVolumeDefinition snapshotVolumeDefinition)
    {
        return getId(
            snapshotVolumeDefinition.getSnapshotDefinition(),
            snapshotVolumeDefinition.getVolumeNumber()
        );
    }

    private String getId(SnapshotDefinition snapshotDefinition, VolumeNumber volumeNumber)
    {
        return getId(
            snapshotDefinition.getResourceName().displayValue,
            snapshotDefinition.getName().displayValue,
            volumeNumber.value
        );
    }

    private String getId(String resName, String snapshotName, int volumeNr)
    {
        return "(ResName=" + resName + " SnapshotName=" + snapshotName + " volumeNr=" + volumeNr + ")";
    }

    private class SizeDriver implements SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(SnapshotVolumeDefinition snapshotVolumeDefinition, Long size)
            throws DatabaseException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(SVD_UPDATE_SIZE))
            {
                errorReporter.logTrace(
                    "Updating VolumeDefinition's Size from [%d] to [%d] %s",
                    snapshotVolumeDefinition.getVolumeSize(dbCtx),
                    size,
                    getId(snapshotVolumeDefinition)
                );

                stmt.setLong(1, size);
                stmt.setString(2, snapshotVolumeDefinition.getResourceName().value);
                stmt.setString(3, snapshotVolumeDefinition.getSnapshotName().value);
                stmt.setInt(4, snapshotVolumeDefinition.getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's Size updated from [%d] to [%d] %s",
                    snapshotVolumeDefinition.getVolumeSize(dbCtx),
                    size,
                    getId(snapshotVolumeDefinition)
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

    private class FlagDriver implements StateFlagsPersistence<SnapshotVolumeDefinition>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(SnapshotVolumeDefinition snapshotVolumeDefinition, long flags)
            throws DatabaseException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(SVD_UPDATE_FLAGS))
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        SnapshotVolumeDefinition.Flags.class,
                        snapshotVolumeDefinition.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        SnapshotVolumeDefinition.Flags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating SnapshotVolumeDefinition's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(snapshotVolumeDefinition)
                );
                stmt.setLong(1, flags);
                stmt.setString(2, snapshotVolumeDefinition.getResourceName().value);
                stmt.setString(3, snapshotVolumeDefinition.getSnapshotName().value);
                stmt.setInt(4, snapshotVolumeDefinition.getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "SnapshotVolumeDefinition's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(snapshotVolumeDefinition)
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

    private static class SnapshotVolumeDefinitionInitMaps implements SnapshotVolumeDefinition.InitMaps
    {
        private final Map<NodeName, SnapshotVolume> snapshotVlmMap;

        private SnapshotVolumeDefinitionInitMaps(Map<NodeName, SnapshotVolume> snapshotVlmMapRef)
        {
            snapshotVlmMap = snapshotVlmMapRef;
        }

        @Override
        public Map<NodeName, SnapshotVolume> getSnapshotVlmMap()
        {
            return snapshotVlmMap;
        }
    }
}
