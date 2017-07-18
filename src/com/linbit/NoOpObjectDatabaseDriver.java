package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public class NoOpObjectDatabaseDriver<T> implements SingleColumnDatabaseDriver<T>
{
    @Override
    public void update(Connection con, T element) throws SQLException
    {
        // no-op
    }
}
