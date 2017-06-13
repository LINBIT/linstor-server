package com.linbit;

import java.sql.Connection;

public interface MapDatabaseDriver<T, U>
{
    public void setConnection(Connection con);

    public void insert(T key, U value);

    public void update(T key, U value);

    public void delete(T key);
}
