package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.core.objects.VolumeDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDefinitionDataDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.core.objects.VolumeDefinitionData} into the database.
     *
     * @param volumeDefinition
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(VolumeDefinitionData volumeDefinition) throws DatabaseException;

    /**
     * Removes the given {@link com.linbit.linstor.core.objects.VolumeDefinitionData} from the database
     *
     * @param volumeDefinition
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(VolumeDefinitionData volumeDefinition) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence();

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver();
}
