package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
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
import java.sql.Types;
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
    private static final String S_NODE_ID = DbConstants.NODE_ID;
    private static final String S_FLAGS = DbConstants.SNAPSHOT_FLAGS;
    private static final String S_LAYER_STACK = DbConstants.LAYER_STACK;

    private static final String S_SELECT_ALL =
        " SELECT " + S_UUID + ", " + S_NODE_NAME + ", " + S_RES_NAME + ", " + S_NAME + ", " +
            S_NODE_ID + ", " + S_FLAGS + ", " + S_LAYER_STACK +
        " FROM " + TBL_SNAPSHOT;

    private static final String S_INSERT =
        " INSERT INTO " + TBL_SNAPSHOT +
        " (" +
            S_UUID + ", " + S_NODE_NAME + ", " + S_RES_NAME + ", " + S_NAME + ", " + S_NODE_ID + ", " +
            S_FLAGS + ", " + S_LAYER_STACK +
        ") VALUES (?, ?, ?, ?, ?, ?, ?)";
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
            stmt.setString(2, snapshot.getNodeName().value);
            stmt.setString(3, snapshot.getResourceName().value);
            stmt.setString(4, snapshot.getSnapshotName().value);
            if (snapshot.getNodeId() == null)
            {
                stmt.setNull(5, Types.INTEGER);
            }
            else
            {
                stmt.setInt(5, snapshot.getNodeId().value);
            }
            stmt.setLong(6, snapshot.getFlags().getFlagsBits(dbCtx));
            GenericDbDriver.setJsonIfNotNull(stmt, 7, snapshot.getLayerStack(dbCtx));
            stmt.executeUpdate();

            errorReporter.logTrace("Snapshot created %s", getId(snapshot));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
        }
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

        NodeId nodeId = getNodeId(resultSet, node, snapshotDefinition);
        snapshot = new SnapshotData(
            java.util.UUID.fromString(resultSet.getString(S_UUID)),
            snapshotDefinition,
            node,
            nodeId,
            resultSet.getLong(S_FLAGS),
            this, transObjFactory, transMgrProvider,
            snapshotVlmMap,
            GenericDbDriver.asDevLayerKindList(
                GenericDbDriver.getAsStringList(resultSet, S_LAYER_STACK)
            )
        );

        errorReporter.logTrace("Snapshot %s created during restore", getId(snapshot));

        return new Pair<>(snapshot, new SnapshotInitMaps(snapshotVlmMap));
    }

    private NodeId getNodeId(ResultSet resultSet, Node node, SnapshotDefinition snapshotDefinition)
        throws SQLException
    {
        NodeId nodeId;
        try
        {
            nodeId = new NodeId(resultSet.getInt(S_NODE_ID));
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "A NodeId of a stored Snapshot in the table %s could not be restored. " +
                        "(NodeName=%s, ResName=%s, SnapshotName=%s, invalid NodeId=%d)",
                    TBL_SNAPSHOT,
                    node.getName(),
                    snapshotDefinition.getResourceName(),
                    snapshotDefinition.getName(),
                    resultSet.getInt(S_NODE_ID)
                ),
                valueOutOfRangeExc
            );
        }
        return nodeId;
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
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(Snapshot snapshot) throws SQLException
    {
        errorReporter.logTrace("Deleting Snapshot %s", getId(snapshot));
        try (PreparedStatement stmt = getConnection().prepareStatement(S_DELETE))
        {
            stmt.setString(1, snapshot.getNodeName().value);
            stmt.setString(2, snapshot.getResourceName().value);
            stmt.setString(3, snapshot.getSnapshotName().value);
            stmt.executeUpdate();
            errorReporter.logTrace("Snapshot deleted %s", getId(snapshot));
        }
    }

    @Override
    public StateFlagsPersistence<Snapshot> getStateFlagsPersistence()
    {
        return flagsDriver;
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
            snapshotDefinition.getResourceName().displayValue,
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
                    "Updating Snapshot's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(snapshot)
                );
                stmt.setLong(1, flags);
                stmt.setString(2, snapshot.getNodeName().value);
                stmt.setString(3, snapshot.getResourceName().value);
                stmt.setString(4, snapshot.getSnapshotName().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "Snapshot's flags updated from [%s] to [%s] %s",
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
