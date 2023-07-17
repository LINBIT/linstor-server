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
     * Updates or creates the given DATA object.
     *
     * @param dataRef
     * @throws DatabaseException
     */
    void upsert(DATA dataRef) throws DatabaseException;

    /**
     * Removes the given DATA from the database
     *
     * @param data
     *
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
