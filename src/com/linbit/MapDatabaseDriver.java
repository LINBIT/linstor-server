package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public interface MapDatabaseDriver<T, U>
{
    public void insert(Connection con, T key, U value) throws SQLException;

    public void update(Connection con, T key, U oldValue, U newValue) throws SQLException;

    public void delete(Connection con, T key, U value) throws SQLException;
}
