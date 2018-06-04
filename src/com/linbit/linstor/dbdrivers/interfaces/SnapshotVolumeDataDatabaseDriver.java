package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.StorPool;

import java.sql.SQLException;

/**
 * Database driver for {@link SnapshotVolume}.
 */
public interface SnapshotVolumeDataDatabaseDriver
{
    /**
     * Loads the {@link SnapshotVolume} specified by the parameter {@code node} and
     * {@code snapshotDefinition}
     *
     * @param snapshot
     *  Part of the primary key specifying the database entry
     * @param snapshotVolumeDefinition
     *  Part of the primary key specifying the database entry
     * @param storPool
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    SnapshotVolume load(
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool,
        boolean logWarnIfNotExists
    )
        throws SQLException;

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
