package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;
import com.linbit.linstor.ResourceConnectionData;

/**
 * Database driver for {@link ResourceConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceConnectionDataDatabaseDriver
{
    /**
     * Persists the given {@link ResourceConnectionData} into the database.
     *
     * @param resConDfnData
     *  The data to be stored (including the primary key)
     * @throws SQLException
     */
    void create(ResourceConnectionData resConDfnData) throws SQLException;

    /**
     * Removes the given {@link ResourceConnectionData} from the database
     *
     * @param resConDfnData
     *  The data identifying the database entry to delete
     * @throws SQLException
     */
    void delete(ResourceConnectionData resConDfnData) throws SQLException;
}
