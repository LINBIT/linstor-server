package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public class NoOpMapDatabaseDriver<T, U> implements MapDatabaseDriver<T, U>
{
    @Override
    public void setConnection(Connection con)
    {
        // no-op
    }

    @Override
    public void insert(T key, U value) throws SQLException
    {
        // no-op
    }

    @Override
    public void update(T key, U value) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(T key) throws SQLException
    {
        // no-op
    }
}
