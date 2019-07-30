package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.KeyValueStoreName;
import com.linbit.linstor.core.objects.KeyValueStoreData;
import com.linbit.linstor.dbdrivers.DatabaseException;

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
     * @throws DatabaseException
     */
    void create(KeyValueStoreData kvs) throws DatabaseException;

    /**
     * Removes the given {@link KeyValueStoreData} from the database
     *
     * @param kvs
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(KeyValueStoreData kvs) throws DatabaseException;

    /**
     * Checks if the stored primary key already exists in the database.
     *
     * @param kvsName
     *  The primary key specifying the database entry
     *
     * @return
     *  True if the data record exists. False otherwise.
     * @throws DatabaseException
     */
    boolean exists(KeyValueStoreName kvsName) throws DatabaseException;

}
