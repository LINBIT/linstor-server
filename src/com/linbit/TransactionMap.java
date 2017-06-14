package com.linbit;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.linbit.drbdmanage.DrbdSqlRuntimeException;

public class TransactionMap<T, U> implements TransactionObject, Map<T, U>
{
    private MapDatabaseDriver<T, U> dbDriver;
    private Map<T, U> map;
    private Map<T, U> oldValues;

    public TransactionMap(Map<T, U> mapRef, MapDatabaseDriver<T, U> driver)
    {
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
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        transMgr.register(this);
        dbDriver.setConnection(transMgr.dbCon);
    }

    @Override
    public void commit()
    {
        oldValues.clear();
    }

    @Override
    public void rollback()
    {
        for (Entry<T, U> entry : oldValues.entrySet())
        {
            T key = entry.getKey();
            U value = entry.getValue();
            if (value == null)
            {
                map.remove(key);
            }
            else
            {
                map.put(key, value);
            }
        }
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
    public U get(Object key)
    {
        return map.get(key);
    }

    @Override
    public U put(T key, U value)
    {
        U oldValue = map.put(key, value);

        cache(key, oldValue);

        if (oldValue == null)
        {
            try
            {
                dbDriver.insert(key, value);
            }
            catch (SQLException sqlExc)
            {
                throw new DrbdSqlRuntimeException(
                    "Inserting to the database from a TransactionMap caused exception",
                    sqlExc
                );
            }
        }
        else
        {
            try
            {
                dbDriver.update(key, value);
            }
            catch (SQLException sqlExc)
            {
                throw new DrbdSqlRuntimeException(
                    "Deleting from the database from a TransactionMap caused exception",
                    sqlExc
                );
            }
        }
        return oldValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public U remove(Object key)
    {
        U oldValue = map.remove(key);

        if (oldValue != null)
        {
            cache((T) key, oldValue);
        }
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends T, ? extends U> m)
    {
        for (Entry<? extends T, ? extends U> entry : m.entrySet())
        {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear()
    {
        for (Entry<T, U> entry : map.entrySet())
        {
            cache(entry.getKey(), entry.getValue());
        }
        map.clear();
    }

    @Override
    public Set<T> keySet()
    {
        return Collections.unmodifiableSet(map.keySet());
    }

    @Override
    public Collection<U> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }

    @Override
    public Set<Entry<T, U>> entrySet()
    {
        return Collections.unmodifiableSet(map.entrySet());
    }

    private void cache(T key, U oldValue)
    {
        if (!oldValues.containsKey(key))
        {
            oldValues.put(key, oldValue);
        }
    }
}
