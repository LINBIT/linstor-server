package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;

public interface GenericDatabaseDriver<DATA>
{
    /**
     * Persists the given DATA object into the database.
     *
     * @param data
     * @throws DatabaseException
     */
    void create(DATA data) throws DatabaseException;

    /**
     * Removes the given DATA from the database
     *
     * @param data
     * @throws DatabaseException
     */
    void delete(DATA data) throws DatabaseException;

    /**
     * Removes all data from the current database table
     *
     * @throws DatabaseException
     */
    void truncate() throws DatabaseException;
}
