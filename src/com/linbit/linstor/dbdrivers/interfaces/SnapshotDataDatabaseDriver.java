package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.Node;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotData;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import java.sql.SQLException;

/**
 * Database driver for {@link Snapshot}.
 */
public interface SnapshotDataDatabaseDriver
{
    /**
     * Loads the {@link Snapshot} specified by the parameter {@code resourceName} and
     * {@code snapshotName}
     *
     * @param node
     *  Part of the primary key specifying the database entry
     * @param snapshotDefinition
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    Snapshot load(
        Node node,
        SnapshotDefinition snapshotDefinition,
        boolean logWarnIfNotExists
    )
        throws SQLException;

    /**
     * Persists the given {@link SnapshotData} into the database.
     *
     * @param snapshot
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(Snapshot snapshot) throws SQLException;

    /**
     * Removes the given {@link SnapshotData} from the database
     *
     * @param snapshot
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(Snapshot snapshot) throws SQLException;
}
