package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.VolumeNumber;

import java.sql.SQLException;

/**
 * Database driver for {@link SnapshotVolumeDefinition}.
 */
public interface SnapshotVolumeDefinitionDatabaseDriver
{
    /**
     * Loads the {@link SnapshotVolumeDefinition} specified by the parameter {@code snapshotDefinition} and
     * {@code volumeNumber}
     *
     * @param snapshotDefinition
     *  Part of the primary key specifying the database entry
     * @param volumeNumber
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    SnapshotVolumeDefinition load(
        SnapshotDefinition snapshotDefinition,
        VolumeNumber volumeNumber,
        boolean logWarnIfNotExists
    )
        throws SQLException;

    /**
     * Persists the given {@link SnapshotVolumeDefinition} into the database.
     *
     * @param snapshotVlmDfn
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(SnapshotVolumeDefinition snapshotVlmDfn) throws SQLException;

    /**
     * Removes the given {@link SnapshotVolumeDefinition} from the database
     *
     * @param snapshotVlmDfn
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(SnapshotVolumeDefinition snapshotVlmDfn) throws SQLException;
}
