package com.linbit;

import java.sql.SQLException;

public class NoOpMapDatabaseDriver<T, U> implements MapDatabaseDriver<T, U>
{
    @Override
    public void insert(T key, U value) throws SQLException
    {
        // no-op
    }

    @Override
    public void update(T key, U oldValue, U value) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(T key, U value) throws SQLException
    {
        // no-op
    }
}
