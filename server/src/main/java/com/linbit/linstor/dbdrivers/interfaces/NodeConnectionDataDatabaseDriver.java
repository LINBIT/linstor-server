package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database driver for {@link NodeConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NodeConnectionDataDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.NodeConnectionData} into the database.
     *
     * @param nodeConDfnData
     *  The data to be stored (including the primary key)
     * @throws DatabaseException
     */
    void create(NodeConnectionData nodeConDfnData) throws DatabaseException;

    /**
     * Removes the given {@link NodeConnectionData} from the database
     *
     * @param nodeConDfnData
     *  The data identifying the database entry to delete
     * @throws DatabaseException
     */
    void delete(NodeConnectionData nodeConDfnData) throws DatabaseException;

}
