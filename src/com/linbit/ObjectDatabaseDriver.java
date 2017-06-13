package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;

public interface ObjectDatabaseDriver<T>
{
    public void setConnection(Connection con);

    public void insert(T element) throws SQLException;

    public void update(T element) throws SQLException;

    public void delete(T element) throws SQLException;
}
