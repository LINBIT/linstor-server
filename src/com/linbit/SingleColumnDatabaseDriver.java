package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public interface SingleColumnDatabaseDriver<T>
{
    public void update(Connection con, T element) throws SQLException;
}
