package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public class NoOpObjectDatabaseDriver<T> implements ObjectDatabaseDriver<T>
{
    @Override
    public void setConnection(Connection con)
    {
        // no-op
    }

    @Override
    public void insert(T element) throws SQLException
    {
        // no-op
    }

    @Override
    public void update(T element) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(T element) throws SQLException
    {
        // no-op
    }
}
