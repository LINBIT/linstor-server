package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.VolumeData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.core.objects.VolumeData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDataDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.core.objects.VolumeData} into the database.
     *
     * @param volume
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(VolumeData volume) throws DatabaseException;

    /**
     * Removes the given {@link com.linbit.linstor.core.objects.VolumeData} from the database
     *
     * @param volume
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(VolumeData volume) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags
     */
    StateFlagsPersistence<VolumeData> getStateFlagsPersistence();
}
