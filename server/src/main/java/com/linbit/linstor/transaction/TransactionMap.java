package com.linbit.linstor.transaction;

import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpMapDatabaseDriver;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TransactionMap<KEY, VALUE>
    extends AbsTransactionObject implements Map<KEY, VALUE>
{
    private MapDatabaseDriver<KEY, VALUE> dbDriver;
    private Map<KEY, VALUE> map;
    private Map<KEY, VALUE> oldValues;

    public TransactionMap(
        Map<KEY, VALUE> mapRef,
        MapDatabaseDriver<KEY, VALUE> driver,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        map = mapRef;
        if (driver == null)
        {
            dbDriver = new NoOpMapDatabaseDriver<>();
        }
        else
        {
            dbDriver = driver;
        }

        oldValues = new HashMap<>();
    }

    @Override
    public void postSetConnection(TransactionMgr transMgrRef)
    {
        // forward transaction manager on keys
        map.keySet().stream()
            .filter(key -> key instanceof TransactionObject)
            .forEach(to -> ((TransactionObject) to).setConnection(transMgrRef));

        // forward transaction manager on values
        map.values().stream()
            .filter(val -> val instanceof TransactionObject)
            .forEach(to -> ((TransactionObject) to).setConnection(transMgrRef));
    }

    @Override
    public void commitImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("commit"));
        oldValues.clear();
    }

    @Override
    public void rollbackImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
        for (Entry<KEY, VALUE> entry : oldValues.entrySet())
        {
            KEY key = entry.getKey();
            VALUE value = entry.getValue();
            if (value == null)
            {
                map.remove(key);
            }
            else
            {
                map.put(key, value);
            }
        }
        oldValues.clear();
    }

    @Override
    public boolean isDirty()
    {
        return !oldValues.isEmpty();
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return map.containsValue(value);
    }

    @Override
    public VALUE get(Object key)
    {
        return map.get(key);
    }

    @Override
    public VALUE put(KEY key, VALUE value)
    {
        VALUE oldValue = map.put(key, value);
        cache(key, value, oldValue);
        return oldValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public VALUE remove(Object key)
    {
        VALUE oldValue = map.remove(key);
        cache((KEY) key, null, oldValue);
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends KEY, ? extends VALUE> srcMap)
    {
        for (Entry<? extends KEY, ? extends VALUE> entry : srcMap.entrySet())
        {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear()
    {
        for (Entry<KEY, VALUE> entry : map.entrySet())
        {
            cache(entry.getKey(), null, entry.getValue());
        }
        map.clear();
    }

    @Override
    public Set<KEY> keySet()
    {
        return Collections.unmodifiableSet(map.keySet());
    }

    @Override
    public Collection<VALUE> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }

    @Override
    public Set<Entry<KEY, VALUE>> entrySet()
    {
        return Collections.unmodifiableSet(map.entrySet());
    }

    private void cache(KEY key, VALUE value, VALUE oldValue)
    {
        if (!hasTransMgr())
        {
            activateTransMgr();
        }
        if (!Objects.equals(value, oldValue))
        {
            if (!oldValues.containsKey(key))
            {
                oldValues.put(key, oldValue);
            }

            if (oldValue == null)
            {
                try
                {
                    dbDriver.insert(key, value);
                }
                catch (DatabaseException sqlExc)
                {
                    throw new LinStorDBRuntimeException(
                        "Inserting to the database from a TransactionMap caused exception",
                        sqlExc
                    );
                }
            }
            else
            {
                if (value == null)
                {
                    try
                    {
                        dbDriver.delete(key, oldValue);
                    }
                    catch (DatabaseException sqlExc)
                    {
                        throw new LinStorDBRuntimeException(
                            "Deleting from the database from a TransactionMap caused exception",
                            sqlExc
                        );
                    }
                }
                else
                {
                    try
                    {
                        dbDriver.update(key, oldValue, value);
                    }
                    catch (DatabaseException sqlExc)
                    {
                        throw new LinStorDBRuntimeException(
                            "Updating the database from a TransactionMap caused exception",
                            sqlExc
                        );
                    }
                }
            }
        }
    }

    @Override
    public boolean equals(Object objRef)
    {
        boolean eq = objRef != null;
        if (eq)
        {
            if (objRef instanceof TransactionObject)
            {
                eq = objRef == this; // do not compare values, just by instance
            }
            else
            {
                eq = objRef.equals(map);
            }
        }

        return eq;
    }

    @Override
    public int hashCode()
    {
        return map.hashCode();
    }

    @Override
    public String toString()
    {
        return "TransactionMap " + map.toString();
    }
}
