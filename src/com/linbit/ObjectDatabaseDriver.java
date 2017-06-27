package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public interface ObjectDatabaseDriver<T>
{
    public void insert(Connection con, T element) throws SQLException;

    public void update(Connection con, T element) throws SQLException;

    public void delete(Connection con, T element) throws SQLException;
}
