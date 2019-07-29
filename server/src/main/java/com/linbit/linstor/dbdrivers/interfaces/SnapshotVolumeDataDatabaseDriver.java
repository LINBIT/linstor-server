package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database driver for {@link SnapshotVolume}.
 */
public interface SnapshotVolumeDataDatabaseDriver
{
    /**
     * Persists the given {@link SnapshotVolume} into the database.
     *
     * @param snapshotVolume
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(SnapshotVolume snapshotVolume) throws DatabaseException;

    /**
     * Removes the given {@link SnapshotVolume} from the database
     *
     * @param snapshotVolume
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(SnapshotVolume snapshotVolume) throws DatabaseException;
}
