package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.SnapshotDefinitionData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link SnapshotDefinitionData}.
 */
public interface SnapshotDefinitionDataDatabaseDriver
{
    /**
     * Persists the given {@link SnapshotDefinitionData} into the database.
     *
     * @param snapshotDefinition
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(SnapshotDefinitionData snapshotDefinition) throws DatabaseException;

    /**
     * Removes the given {@link SnapshotDefinitionData} from the database
     *
     * @param snapshotDefinition
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(SnapshotDefinitionData snapshotDefinition) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<SnapshotDefinitionData> getStateFlagsPersistence();
}
