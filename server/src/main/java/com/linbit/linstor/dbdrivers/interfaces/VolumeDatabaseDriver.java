package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.core.objects.AbsVolume}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.core.objects.AbsVolume} into the database.
     *
     * @param volume
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(Volume volume) throws DatabaseException;

    /**
     * Removes the given {@link com.linbit.linstor.core.objects.AbsVolume} from the database
     *
     * @param volume
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(Volume volume) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags
     */
    StateFlagsPersistence<Volume> getStateFlagsPersistence();
}
