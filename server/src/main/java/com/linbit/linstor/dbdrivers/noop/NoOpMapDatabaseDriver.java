package com.linbit.linstor.dbdrivers.noop;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;

import java.util.Map;

public class NoOpMapDatabaseDriver<PARENT, K, V> implements MapDatabaseDriver<PARENT, K, V>
{
    @Override
    public void insert(PARENT parent, Map<K, V> backingMap, K key, V value) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void update(PARENT parent, Map<K, V> backingMap, K key, V oldValue, V value) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(PARENT parent, Map<K, V> backingMap, K key, V value) throws DatabaseException
    {
        // no-op
    }
}
