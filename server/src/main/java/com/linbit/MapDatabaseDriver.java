package com.linbit;

import java.sql.SQLException;

public interface MapDatabaseDriver<T, U>
{
    void insert(T key, U value) throws SQLException;

    void update(T key, U oldValue, U newValue) throws SQLException;

    void delete(T key, U value) throws SQLException;
}
