package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

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
     * @throws DatabaseException
     */
    void create(SnapshotVolumeDefinition snapshotVlmDfn) throws DatabaseException;

    /**
     * Removes the given {@link SnapshotVolumeDefinition} from the database
     *
     * @param snapshotVlmDfn
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(SnapshotVolumeDefinition snapshotVlmDfn) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> getVolumeSizeDriver();

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<SnapshotVolumeDefinition> getStateFlagsPersistence();
}
