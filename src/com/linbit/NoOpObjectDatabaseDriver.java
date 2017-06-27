package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public class NoOpObjectDatabaseDriver<T> implements ObjectDatabaseDriver<T>
{
    @Override
    public void insert(Connection con, T element) throws SQLException
    {
        // no-op
    }

    @Override
    public void update(Connection con, T element) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(Connection con, T element) throws SQLException
    {
        // no-op
    }
}
