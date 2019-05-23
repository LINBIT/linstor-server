package com.linbit;

import com.linbit.linstor.dbdrivers.DatabaseException;

public class NoOpMapDatabaseDriver<T, U> implements MapDatabaseDriver<T, U>
{
    @Override
    public void insert(T key, U value) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void update(T key, U oldValue, U value) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(T key, U value) throws DatabaseException
    {
        // no-op
    }
}
