package com.linbit.linstor.dbdrivers.interfaces.updater;

import com.linbit.linstor.dbdrivers.DatabaseException;

public interface MapDatabaseDriver<T, U>
{
    void insert(T key, U value) throws DatabaseException;

    void update(T key, U oldValue, U newValue) throws DatabaseException;

    void delete(T key, U value) throws DatabaseException;
}
