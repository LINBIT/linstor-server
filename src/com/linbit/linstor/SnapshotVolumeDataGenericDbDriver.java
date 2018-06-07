package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.Triple;

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

@Singleton
public class SnapshotVolumeDataGenericDbDriver implements SnapshotVolumeDataDatabaseDriver
{
    private static final String TBL_SNAPSHOT = DbConstants.TBL_SNAPSHOT_VOLUMES;

    private static final String SV_UUID = DbConstants.UUID;
    private static final String SV_NODE_NAME = DbConstants.NODE_NAME;
    private static final String SV_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String SV_SNAPSHOT_NAME = DbConstants.SNAPSHOT_NAME;
    private static final String SV_VLM_NR = DbConstants.VLM_NR;
    private static final String SV_STOR_POOL_NAME = DbConstants.STOR_POOL_NAME;

    private static final String SV_SELECT_ALL =
        " SELECT " + SV_UUID + ", " + SV_NODE_NAME + ", " + SV_RES_NAME + ", " + SV_SNAPSHOT_NAME + ", " +
            SV_VLM_NR + ", " + SV_STOR_POOL_NAME +
        " FROM " + TBL_SNAPSHOT;
    private static final String SV_SELECT =
        SV_SELECT_ALL +
        " WHERE " + SV_NODE_NAME + " = ? AND " +
            SV_RES_NAME + " = ? AND " +
            SV_SNAPSHOT_NAME + " = ? AND " +
            SV_VLM_NR + " = ?";

    private static final String SV_INSERT =
        " INSERT INTO " + TBL_SNAPSHOT +
        " (" +
            SV_UUID + ", " + SV_NODE_NAME + ", " + SV_RES_NAME + ", " + SV_SNAPSHOT_NAME + ", " +
            SV_VLM_NR + ", " + SV_STOR_POOL_NAME +
        ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String SV_DELETE =
        " DELETE FROM " + TBL_SNAPSHOT +
        " WHERE " + SV_NODE_NAME + " = ? AND " +
            SV_RES_NAME + " = ? AND " +
            SV_SNAPSHOT_NAME + " = ? AND " +
            SV_VLM_NR + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeDataGenericDbDriver(
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
    public void create(SnapshotVolume snapshotVolume) throws SQLException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(SV_INSERT))
        {
            errorReporter.logTrace("Creating SnapshotVolume %s", getId(snapshotVolume));

            Snapshot snapshot = snapshotVolume.getSnapshot();
            stmt.setString(1, snapshotVolume.getUuid().toString());
            stmt.setString(2, snapshot.getNodeName().value);
            stmt.setString(3, snapshot.getResourceName().value);
            stmt.setString(4, snapshot.getSnapshotName().value);
            stmt.setInt(5, snapshotVolume.getVolumeNumber().value);
            stmt.setString(6, snapshotVolume.getStorPool(dbCtx).getName().value);

            stmt.executeUpdate();

            errorReporter.logTrace("SnapshotVolume created %s", getId(snapshotVolume));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public SnapshotVolume load(
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading SnapshotVolume %s", getId(snapshot, snapshotVolumeDefinition));
        SnapshotVolume ret;
        try (PreparedStatement stmt = getConnection().prepareStatement(SV_SELECT))
        {
            SnapshotDefinition snapshotDefinition = snapshot.getSnapshotDefinition();

            stmt.setString(1, snapshot.getNodeName().value);
            stmt.setString(2, snapshotDefinition.getResourceName().value);
            stmt.setString(3, snapshotDefinition.getName().value);
            stmt.setInt(4, snapshotVolumeDefinition.getVolumeNumber().value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                ret = cacheGet(snapshot, snapshotVolumeDefinition);
                if (ret == null)
                {
                    if (resultSet.next())
                    {
                        ret = restoreSnapshotVolume(
                            resultSet,
                            snapshot,
                            snapshotVolumeDefinition,
                            storPool
                        );
                        errorReporter.logTrace("SnapshotVolume loaded %s", getId(snapshot, snapshotVolumeDefinition));
                    }
                    else
                    if (logWarnIfNotExists)
                    {
                        errorReporter.logWarning(
                            "Requested SnapshotVolume %s could not be found in the Database",
                            getId(snapshot, snapshotVolumeDefinition)
                        );
                    }
                }
            }
        }
        return ret;
    }

    private SnapshotVolume restoreSnapshotVolume(
        ResultSet resultSet,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool
        )
        throws SQLException
    {
        errorReporter.logTrace("Restoring SnapshotVolume %s", getId(snapshot, snapshotVolumeDefinition));
        SnapshotVolume snapshotVolume;

        snapshotVolume = cacheGet(snapshot, snapshotVolumeDefinition);

        if (snapshotVolume == null)
        {
            snapshotVolume = new SnapshotVolumeData(
                java.util.UUID.fromString(resultSet.getString(SV_UUID)),
                snapshot,
                snapshotVolumeDefinition,
                storPool,
                this, transObjFactory, transMgrProvider
            );

            errorReporter.logTrace("SnapshotVolume %s created during restore", getId(snapshotVolume));
        }
        else
        {
            errorReporter.logTrace("SnapshotVolume %s restored from cache", getId(snapshotVolume));
        }

        return snapshotVolume;
    }


    public List<SnapshotVolume> loadAll(
        Map<Triple<NodeName, ResourceName, SnapshotName>, ? extends Snapshot> snapshotMap,
        Map<Triple<ResourceName, SnapshotName, VolumeNumber>, ? extends SnapshotVolumeDefinition> snapshotVlmDfnMap,
        Map<Pair<NodeName, StorPoolName>, ? extends StorPool> storPoolMap
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading all SnapshotVolumes");
        List<SnapshotVolume> ret = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SV_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    NodeName nodeName;
                    ResourceName rscName;
                    SnapshotName snapshotName;
                    VolumeNumber volumeNumber;
                    StorPoolName storPoolName;
                    int vlmNr = resultSet.getInt(SV_VLM_NR);
                    try
                    {
                        nodeName = new NodeName(resultSet.getString(SV_NODE_NAME));
                        rscName = new ResourceName(resultSet.getString(SV_RES_NAME));
                        snapshotName = new SnapshotName(resultSet.getString(SV_SNAPSHOT_NAME));
                        volumeNumber = new VolumeNumber(vlmNr);
                        storPoolName = new StorPoolName(resultSet.getString(SV_STOR_POOL_NAME));
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            TBL_SNAPSHOT + " contained invalid name: " + exc.invalidName,
                            exc
                        );
                    }
                    catch (ValueOutOfRangeException exc)
                    {
                        throw new ImplementationError(
                            TBL_SNAPSHOT + " contained invalid volume number: " + vlmNr,
                            exc
                        );
                    }

                    SnapshotVolume snapshotVolume = restoreSnapshotVolume(
                        resultSet,
                        snapshotMap.get(new Triple<>(nodeName, rscName, snapshotName)),
                        snapshotVlmDfnMap.get(new Triple<>(rscName, snapshotName, volumeNumber)),
                        storPoolMap.get(new Pair<>(nodeName, storPoolName))
                    );

                    ret.add(snapshotVolume);

                    errorReporter.logTrace("SnapshotVolume created %s", getId(snapshotVolume));
                }
            }
        }
        return ret;
    }

    @Override
    public void delete(SnapshotVolume snapshotVolume) throws SQLException
    {
        errorReporter.logTrace("Deleting SnapshotVolume %s", getId(snapshotVolume));
        try (PreparedStatement stmt = getConnection().prepareStatement(SV_DELETE))
        {
            Snapshot snapshot = snapshotVolume.getSnapshot();
            SnapshotDefinition snapshotDefinition = snapshot.getSnapshotDefinition();

            stmt.setString(1, snapshot.getNodeName().value);
            stmt.setString(2, snapshotDefinition.getResourceName().value);
            stmt.setString(3, snapshotDefinition.getName().value);
            stmt.setInt(4, snapshotVolume.getVolumeNumber().value);
            stmt.executeUpdate();
            errorReporter.logTrace("SnapshotVolume deleted %s", getId(snapshotVolume));
        }
    }

    private SnapshotVolume cacheGet(Snapshot snapshot, SnapshotVolumeDefinition snapshotVolumeDefinition)
    {
        return snapshot.getSnapshotVolume(snapshotVolumeDefinition.getVolumeNumber());
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(SnapshotVolume snapshotVolume)
    {
        return getId(
            snapshotVolume.getSnapshot(),
            snapshotVolume.getSnapshotVolumeDefinition()
        );
    }

    private String getId(Snapshot snapshot, SnapshotVolumeDefinition snapshotVolumeDefinition)
    {
        return getId(
            snapshot.getNodeName().displayValue,
            snapshot.getResourceName().displayValue,
            snapshot.getSnapshotName().displayValue,
            snapshotVolumeDefinition.getVolumeNumber().value
        );
    }

    private String getId(String nodeName, String resName, String snapshotName, int volumeNr)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + " SnapshotName=" + snapshotName +
            " VolumeNr=" + volumeNr + ")";
    }
}
