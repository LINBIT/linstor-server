package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.Snapshot.SnapshotFlags;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
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
public class SnapshotDataGenericDbDriver implements SnapshotDataDatabaseDriver
{
    private static final String TBL_SNAPSHOT = DbConstants.TBL_SNAPSHOTS;

    private static final String S_UUID = DbConstants.UUID;
    private static final String S_NODE_NAME = DbConstants.NODE_NAME;
    private static final String S_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String S_NAME = DbConstants.SNAPSHOT_NAME;
    private static final String S_FLAGS = DbConstants.SNAPSHOT_FLAGS;

    private static final String S_SELECT_ALL =
        " SELECT " + S_UUID + ", " + S_NODE_NAME + ", " + S_RES_NAME + ", " + S_NAME + ", " + S_FLAGS +
        " FROM " + TBL_SNAPSHOT;
    private static final String S_SELECT =
        S_SELECT_ALL +
        " WHERE " + S_NODE_NAME + " = ? AND " +
            S_RES_NAME + " = ? AND " +
            S_NAME + " = ?";

    private static final String S_INSERT =
        " INSERT INTO " + TBL_SNAPSHOT +
        " (" +
            S_UUID + ", " + S_NODE_NAME + ", " + S_RES_NAME + ", " + S_NAME + ", " + S_FLAGS +
        ") VALUES (?, ?, ?, ?, ?)";
    private static final String S_UPDATE_FLAGS =
        " UPDATE " + TBL_SNAPSHOT +
        " SET " + S_FLAGS + " = ? " +
        " WHERE " + S_NODE_NAME + " = ? AND " +
            S_RES_NAME + " = ? AND " +
            S_NAME + " = ?";
    private static final String S_DELETE =
        " DELETE FROM " + TBL_SNAPSHOT +
        " WHERE " + S_NODE_NAME + " = ? AND " +
            S_RES_NAME + " = ? AND " +
            S_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final FlagDriver flagsDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotDataGenericDbDriver(
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
        flagsDriver = new FlagDriver();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(Snapshot snapshot) throws SQLException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(S_INSERT))
        {
            errorReporter.logTrace("Creating Snapshot %s", getId(snapshot));

            stmt.setString(1, snapshot.getUuid().toString());
            stmt.setString(2, snapshot.getNode().getName().value);
            stmt.setString(3, snapshot.getSnapshotDefinition().getResourceDefinition().getName().value);
            stmt.setString(4, snapshot.getSnapshotDefinition().getName().value);
            stmt.setLong(5, snapshot.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace("Snapshot created %s", getId(snapshot));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public Snapshot load(
        Node node,
        SnapshotDefinition snapshotDefinition,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading Snapshot %s", getId(node, snapshotDefinition));
        Snapshot ret;
        try (PreparedStatement stmt = getConnection().prepareStatement(S_SELECT))
        {
            stmt.setString(1, node.getName().value);
            stmt.setString(2, snapshotDefinition.getResourceDefinition().getName().value);
            stmt.setString(3, snapshotDefinition.getName().value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                ret = cacheGet(node, snapshotDefinition);
                if (ret == null)
                {
                    if (resultSet.next())
                    {
                        ret = restoreSnapshot(
                            resultSet,
                            node,
                            snapshotDefinition
                        ).objA;
                        errorReporter.logTrace("Snapshot loaded %s", getId(node, snapshotDefinition));
                    }
                    else
                    if (logWarnIfNotExists)
                    {
                        errorReporter.logWarning(
                            "Requested Snapshot %s could not be found in the Database",
                            getId(node, snapshotDefinition)
                        );
                    }
                }
            }
        }
        return ret;
    }

    private Pair<Snapshot, Snapshot.InitMaps> restoreSnapshot(
        ResultSet resultSet,
        Node node,
        SnapshotDefinition snapshotDefinition
    )
        throws SQLException
    {
        errorReporter.logTrace("Restoring Snapshot %s", getId(node, snapshotDefinition));
        Snapshot snapshot;

        Map<VolumeNumber, SnapshotVolume> snapshotVlmMap = new TreeMap<>();

        snapshot = new SnapshotData(
            java.util.UUID.fromString(resultSet.getString(S_UUID)),
            snapshotDefinition,
            node,
            resultSet.getLong(S_FLAGS),
            this, transObjFactory, transMgrProvider,
            snapshotVlmMap
        );

        errorReporter.logTrace("Snapshot %s created during restore", getId(snapshot));

        return new Pair<>(snapshot, new SnapshotInitMaps(snapshotVlmMap));
    }


    public Map<Snapshot, Snapshot.InitMaps> loadAll(
        Map<NodeName, ? extends Node> nodeMap,
        Map<Pair<ResourceName, SnapshotName>, ? extends SnapshotDefinition> snapshotDfnMap
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading all Snapshots");
        Map<Snapshot, Snapshot.InitMaps> loadedSnapshots = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(S_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    NodeName nodeName;
                    ResourceName rscName;
                    SnapshotName snapshotName;
                    try
                    {
                        nodeName = new NodeName(resultSet.getString(S_NODE_NAME));
                        rscName = new ResourceName(resultSet.getString(S_RES_NAME));
                        snapshotName = new SnapshotName(resultSet.getString(S_NAME));
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            TBL_SNAPSHOT + " contained invalid name: " + exc.invalidName,
                            exc
                        );
                    }

                    Pair<Snapshot, Snapshot.InitMaps> pair = restoreSnapshot(
                        resultSet,
                        nodeMap.get(nodeName),
                        snapshotDfnMap.get(new Pair<>(rscName, snapshotName))
                    );

                    loadedSnapshots.put(pair.objA, pair.objB);
                }
            }
        }
        return loadedSnapshots;
    }

    @Override
    public void delete(Snapshot snapshot) throws SQLException
    {
        errorReporter.logTrace("Deleting Snapshot %s", getId(snapshot));
        try (PreparedStatement stmt = getConnection().prepareStatement(S_DELETE))
        {
            stmt.setString(1, snapshot.getNode().getName().value);
            stmt.setString(2, snapshot.getSnapshotDefinition().getResourceDefinition().getName().value);
            stmt.setString(3, snapshot.getSnapshotDefinition().getName().value);
            stmt.executeUpdate();
            errorReporter.logTrace("Snapshot deleted %s", getId(snapshot));
        }
    }

    @Override
    public StateFlagsPersistence<Snapshot> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    private Snapshot cacheGet(Node node, SnapshotDefinition snapshotDefinition)
    {
        return snapshotDefinition.getSnapshot(node.getName());
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(Snapshot snapshot)
    {
        return getId(
            snapshot.getNode(),
            snapshot.getSnapshotDefinition()
        );
    }

    private String getId(Node node, SnapshotDefinition snapshotDefinition)
    {
        return getId(
            node.getName().displayValue,
            snapshotDefinition.getResourceDefinition().getName().displayValue,
            snapshotDefinition.getName().displayValue
        );
    }

    private String getId(String nodeName, String resName, String snapshotName)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + " SnapshotName=" + snapshotName + ")";
    }

    private class FlagDriver implements StateFlagsPersistence<Snapshot>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(Snapshot snapshot, long flags)
            throws SQLException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(S_UPDATE_FLAGS))
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        SnapshotFlags.class,
                        snapshot.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        SnapshotFlags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating SnapshotDefinition's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(snapshot)
                );
                stmt.setLong(1, flags);
                stmt.setString(2, snapshot.getNode().getName().value);
                stmt.setString(3, snapshot.getSnapshotDefinition().getResourceDefinition().getName().value);
                stmt.setString(4, snapshot.getSnapshotDefinition().getName().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "SnapshotDefinition's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(snapshot)
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }

    private static class SnapshotInitMaps implements Snapshot.InitMaps
    {
        private final Map<VolumeNumber, SnapshotVolume> snapshotVlmMap;

        private SnapshotInitMaps(Map<VolumeNumber, SnapshotVolume> snapshotVlmMapRef)
        {
            snapshotVlmMap = snapshotVlmMapRef;
        }

        @Override
        public Map<VolumeNumber, SnapshotVolume> getSnapshotVlmMap()
        {
            return snapshotVlmMap;
        }
    }
}
