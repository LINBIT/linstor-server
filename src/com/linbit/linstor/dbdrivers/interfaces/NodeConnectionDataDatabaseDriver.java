package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;
import com.linbit.linstor.NodeConnectionData;

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
     * @throws SQLException
     */
    void create(NodeConnectionData nodeConDfnData) throws SQLException;

    /**
     * Removes the given {@link NodeConnectionData} from the database
     *
     * @param nodeConDfnData
     *  The data identifying the database entry to delete
     * @throws SQLException
     */
    void delete(NodeConnectionData nodeConDfnData) throws SQLException;

}
