package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public interface MapDatabaseDriver<T, U>
{
    void insert(Connection con, T key, U value) throws SQLException;

    void update(Connection con, T key, U oldValue, U newValue) throws SQLException;

    void delete(Connection con, T key, U value) throws SQLException;
}
