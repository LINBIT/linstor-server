package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

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
     * @throws DatabaseException
     */
    void create(Snapshot snapshot) throws DatabaseException;

    /**
     * Removes the given {@link SnapshotData} from the database
     *
     * @param snapshot
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(Snapshot snapshot) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<Snapshot> getStateFlagsPersistence();
}
