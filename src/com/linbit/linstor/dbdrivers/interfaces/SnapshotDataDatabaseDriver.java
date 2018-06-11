package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotData;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import java.sql.SQLException;

/**
 * Database driver for {@link Snapshot}.
 */
public interface SnapshotDataDatabaseDriver
{
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

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<Snapshot> getStateFlagsPersistence();
}
