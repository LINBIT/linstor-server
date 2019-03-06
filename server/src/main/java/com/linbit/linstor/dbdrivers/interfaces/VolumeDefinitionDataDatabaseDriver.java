package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.VolumeDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDefinitionDataDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.VolumeDefinitionData} into the database.
     *
     * @param volumeDefinition
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(VolumeDefinitionData volumeDefinition) throws SQLException;

    /**
     * Removes the given {@link com.linbit.linstor.VolumeDefinitionData} from the database
     *
     * @param volumeDefinition
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(VolumeDefinitionData volumeDefinition) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence();

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver();
}
