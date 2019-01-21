package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.linstor.KeyValueStoreData;
import com.linbit.linstor.KeyValueStoreName;

/**
 * Database driver for {@link KeyValueStoreData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface KeyValueStoreDataDatabaseDriver
{
    /**
     * Persists the given {@link KeyValueStoreData} into the database.
     *
     * @param kvs
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(KeyValueStoreData kvs) throws SQLException;

    /**
     * Removes the given {@link KeyValueStoreData} from the database
     *
     * @param kvs
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(KeyValueStoreData kvs) throws SQLException;

    /**
     * Checks if the stored primary key already exists in the database.
     *
     * @param kvsName
     *  The primary key specifying the database entry
     *
     * @return
     *  True if the data record exists. False otherwise.
     * @throws SQLException
     */
    boolean exists(KeyValueStoreName kvsName) throws SQLException;

}
