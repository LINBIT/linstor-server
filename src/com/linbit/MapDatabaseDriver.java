package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public interface MapDatabaseDriver<T, U>
{
    public void setConnection(Connection con);

    public void insert(T key, U value) throws SQLException;

    public void update(T key, U value) throws SQLException;

    public void delete(T key) throws SQLException;
}
