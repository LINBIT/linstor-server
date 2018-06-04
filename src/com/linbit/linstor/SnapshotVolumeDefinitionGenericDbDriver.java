package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
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
public class SnapshotVolumeDefinitionGenericDbDriver implements SnapshotVolumeDefinitionDatabaseDriver
{
    private static final String TBL_SNAPSHOT_VOLUME_DEFINITIONS = DbConstants.TBL_SNAPSHOT_VOLUME_DEFINITIONS;

    private static final String SVD_UUID = DbConstants.UUID;
    private static final String SVD_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String SVD_SNAPSHOT_NAME = DbConstants.SNAPSHOT_NAME;
    private static final String SVD_VLM_NR = DbConstants.VLM_NR;

    private static final String SVD_SELECT_ALL =
        " SELECT " + SVD_UUID + ", " + SVD_RES_NAME + ", " + SVD_SNAPSHOT_NAME + ", " + SVD_VLM_NR +
        " FROM " + TBL_SNAPSHOT_VOLUME_DEFINITIONS;
    private static final String SVD_SELECT =
        SVD_SELECT_ALL +
        " WHERE " + SVD_RES_NAME + " = ? AND " +
            SVD_SNAPSHOT_NAME + " = ? AND " +
            SVD_VLM_NR + " = ?";

    private static final String SVD_INSERT =
        " INSERT INTO " + TBL_SNAPSHOT_VOLUME_DEFINITIONS +
        " (" +
            SVD_UUID + ", " + SVD_RES_NAME + ", " + SVD_SNAPSHOT_NAME + ", " + SVD_VLM_NR +
        ") VALUES (?, ?, ?, ?)";
    private static final String SVD_DELETE =
        " DELETE FROM " + TBL_SNAPSHOT_VOLUME_DEFINITIONS +
        " WHERE " + SVD_RES_NAME + " = ? AND " +
            SVD_SNAPSHOT_NAME + " = ? AND " +
            SVD_VLM_NR + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeDefinitionGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(SnapshotVolumeDefinition snapshotVolumeDefinition) throws SQLException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(SVD_INSERT))
        {
            errorReporter.logTrace("Creating SnapshotVolumeDefinition %s", getId(snapshotVolumeDefinition));

            stmt.setString(1, snapshotVolumeDefinition.getUuid().toString());
            stmt.setString(2, snapshotVolumeDefinition.getSnapshotDefinition().getResourceDefinition().getName().value);
            stmt.setString(3, snapshotVolumeDefinition.getSnapshotDefinition().getName().value);
            stmt.setInt(4, snapshotVolumeDefinition.getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("SnapshotVolumeDefinition created %s", getId(snapshotVolumeDefinition));
        }
    }

    @Override
    public SnapshotVolumeDefinition load(
        SnapshotDefinition snapshotDefinition,
        VolumeNumber volumeNumber,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading SnapshotVolumeDefinition %s", getId(snapshotDefinition, volumeNumber));
        SnapshotVolumeDefinition ret;
        try (PreparedStatement stmt = getConnection().prepareStatement(SVD_SELECT))
        {
            stmt.setString(1, snapshotDefinition.getResourceDefinition().getName().value);
            stmt.setString(2, snapshotDefinition.getName().value);
            stmt.setInt(3, volumeNumber.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                ret = cacheGet(snapshotDefinition, volumeNumber);
                if (ret == null)
                {
                    if (resultSet.next())
                    {
                        ret = restoreSnapshotVolumeDefinition(
                            resultSet,
                            snapshotDefinition,
                            volumeNumber
                        ).objA;
                        errorReporter.logTrace("SnapshotVolumeDefinition loaded %s", getId(snapshotDefinition, volumeNumber));
                    }
                    else
                    if (logWarnIfNotExists)
                    {
                        errorReporter.logWarning(
                            "Requested SnapshotVolumeDefinition %s could not be found in the Database",
                            getId(snapshotDefinition, volumeNumber)
                        );
                    }
                }
            }
        }
        return ret;
    }

    private Pair<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> restoreSnapshotVolumeDefinition(
        ResultSet resultSet,
        SnapshotDefinition snapshotDefinition,
        VolumeNumber volumeNumber
    )
        throws SQLException
    {
        errorReporter.logTrace("Restoring SnapshotVolumeDefinition %s", getId(snapshotDefinition, volumeNumber));
        SnapshotVolumeDefinition snapshotVolumeDefinition;

        Map<NodeName, SnapshotVolume> snapshotVlmMap = new TreeMap<>();

        snapshotVolumeDefinition = new SnapshotVolumeDefinitionData(
            java.util.UUID.fromString(resultSet.getString(SVD_UUID)),
            snapshotDefinition,
            volumeNumber,
            this,
            transObjFactory,
            transMgrProvider,
            snapshotVlmMap
        );

        errorReporter.logTrace("SnapshotVolumeDefinition %s created during restore", getId(snapshotVolumeDefinition));

        return new Pair<>(snapshotVolumeDefinition, new SnapshotVolumeDefinitionInitMaps(snapshotVlmMap));
    }


    public Map<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> loadAll(
        Map<Pair<ResourceName, SnapshotName>, ? extends SnapshotDefinition> snapshotDfnMap
    )
        throws SQLException
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
                            volumeNumber
                        );

                    loadedSnapshotVlmDfns.put(pair.objA, pair.objB);
                }
            }
        }
        return loadedSnapshotVlmDfns;
    }

    @Override
    public void delete(SnapshotVolumeDefinition snapshotVolumeDefinition) throws SQLException
    {
        errorReporter.logTrace("Deleting SnapshotVolumeDefinition %s", getId(snapshotVolumeDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(SVD_DELETE))
        {
            stmt.setString(1,
                snapshotVolumeDefinition.getSnapshotDefinition().getResourceDefinition().getName().value);
            stmt.setString(2, snapshotVolumeDefinition.getSnapshotDefinition().getName().value);
            stmt.setInt(3, snapshotVolumeDefinition.getVolumeNumber().value);
            stmt.executeUpdate();
            errorReporter.logTrace("SnapshotVolumeDefinition deleted %s", getId(snapshotVolumeDefinition));
        }
    }

    private SnapshotVolumeDefinition cacheGet(SnapshotDefinition snapshotDefinition, VolumeNumber volumeNumber)
    {
        return snapshotDefinition.getSnapshotVolumeDefinition(volumeNumber);
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
            snapshotDefinition.getResourceDefinition().getName().displayValue,
            snapshotDefinition.getName().displayValue,
            volumeNumber.value
        );
    }

    private String getId(String resName, String snapshotName, int volumeNr)
    {
        return "(ResName=" + resName + " SnapshotName=" + snapshotName + " volumeNr=" + volumeNr + ")";
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
