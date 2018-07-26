package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.SnapshotVolume;
import java.sql.SQLException;

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
     * @throws SQLException
     */
    void create(SnapshotVolume snapshotVolume) throws SQLException;

    /**
     * Removes the given {@link SnapshotVolume} from the database
     *
     * @param snapshotVolume
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(SnapshotVolume snapshotVolume) throws SQLException;
}
