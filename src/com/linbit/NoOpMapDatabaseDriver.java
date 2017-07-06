package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public class NoOpMapDatabaseDriver<T, U> implements MapDatabaseDriver<T, U>
{
    @Override
    public void insert(Connection con, T key, U value) throws SQLException
    {
        // no-op
    }

    @Override
    public void update(Connection con, T key, U oldValue, U value) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(Connection con, T key, U value) throws SQLException
    {
        // no-op
    }
}
