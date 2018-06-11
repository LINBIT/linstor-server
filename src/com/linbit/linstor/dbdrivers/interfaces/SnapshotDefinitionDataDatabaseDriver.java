package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.SnapshotDefinitionData;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import java.sql.SQLException;

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
     * @throws SQLException
     */
    void create(SnapshotDefinitionData snapshotDefinition) throws SQLException;

    /**
     * Removes the given {@link SnapshotDefinitionData} from the database
     *
     * @param snapshotDefinition
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(SnapshotDefinitionData snapshotDefinition) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<SnapshotDefinitionData> getStateFlagsPersistence();
}
