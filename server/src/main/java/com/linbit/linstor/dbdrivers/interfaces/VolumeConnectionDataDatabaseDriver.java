package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;
import com.linbit.linstor.VolumeConnectionData;

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
     * @throws SQLException
     */
    void create(VolumeConnectionData conDfnData) throws SQLException;

    /**
     * Removes the given {@link com.linbit.linstor.VolumeConnectionData} from the database
     *
     * @param conDfnData
     *  The data identifying the database entry to delete
     *
     * @throws SQLException
     */
    void delete(VolumeConnectionData conDfnData) throws SQLException;
}
