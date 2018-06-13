package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import java.sql.SQLException;

/**
 * Database driver for {@link SnapshotVolumeDefinition}.
 */
public interface SnapshotVolumeDefinitionDatabaseDriver
{
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

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> getVolumeSizeDriver();

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<SnapshotVolumeDefinition> getStateFlagsPersistence();
}
