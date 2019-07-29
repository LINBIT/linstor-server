package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.VolumeConnectionData;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database driver for {@link com.linbit.linstor.VolumeConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeConnectionDataDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.VolumeConnectionData} into the database.
     *
     * @param conDfnData
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(VolumeConnectionData conDfnData) throws DatabaseException;

    /**
     * Removes the given {@link com.linbit.linstor.VolumeConnectionData} from the database
     *
     * @param conDfnData
     *  The data identifying the database entry to delete
     *
     * @throws DatabaseException
     */
    void delete(VolumeConnectionData conDfnData) throws DatabaseException;
}
