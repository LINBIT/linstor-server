package com.linbit.linstor.dbdrivers.interfaces.updater;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;

import java.util.Map;

public interface MapDatabaseDriver<PARENT, K, V>
{
    /**
     * Called after the backing map received the new entry.
     */
    void insert(PARENT parent, Map<K, V> backingMap, K key, @Nullable V value) throws DatabaseException;

    /**
     * Called after the backing map received the updated entry.
     */
    void update(PARENT parent, Map<K, V> backingMap, K key, V oldValue, V newValue) throws DatabaseException;

    /**
     * Called after the given entry was deleted from the backing map.
     */
    void delete(PARENT parent, Map<K, V> backingMap, K key, V value) throws DatabaseException;
}
