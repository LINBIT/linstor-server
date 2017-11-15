package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.linbit.drbdmanage.DrbdSqlRuntimeException;

public class TransactionMap<T, U> implements TransactionObject, Map<T, U>
{
    private MapDatabaseDriver<T, U> dbDriver;
    private Map<T, U> map;
    private Map<T, U> oldValues;

    private Connection con;

    private boolean initialized = false;
    private TransactionMgr transMgr;

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
    public void initialized()
    {
        initialized = true;
    }

    @Override
    public void setConnection(TransactionMgr transMgr)
    {
        if (isDbCacheDirty())
        {
            throw new ImplementationError("setConnection was called AFTER data was manipulated", null);
        }
        if (transMgr != null)
        {
            transMgr.register(this);
            con = transMgr.dbCon;
            this.transMgr = transMgr;
        }
        else
        {
            con = null;
        }
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
    public boolean isDbCacheDirty()
    {
        return !(dbDriver instanceof NoOpMapDatabaseDriver) && isDirty();
    }

    @Override
    public boolean hasTransMgr()
    {
        return transMgr != null;
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
        cache(key, value, oldValue);
        return oldValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public U remove(Object key)
    {
        U oldValue = map.remove(key);
        cache((T) key, null, oldValue);
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
            cache(entry.getKey(), null, entry.getValue());
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


    private void cache(T key, U value, U oldValue)
    {
        if (initialized && !Objects.equals(value, oldValue))
        {
            if (!oldValues.containsKey(key))
            {
                oldValues.put(key, oldValue);
            }

            if (con != null)
            {
                if (oldValue == null)
                {
                    try
                    {
                        dbDriver.insert(con, key, value);
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
                    if (value == null)
                    {
                        try
                        {
                            dbDriver.delete(con, key, oldValue);
                        }
                        catch (SQLException sqlExc)
                        {
                            throw new DrbdSqlRuntimeException(
                                "Deleting from the database from a TransactionMap caused exception",
                                sqlExc
                            );
                        }
                    }
                    else
                    {
                        try
                        {
                            dbDriver.update(con, key, oldValue, value);
                        }
                        catch (SQLException sqlExc)
                        {
                            throw new DrbdSqlRuntimeException(
                                "Updating the database from a TransactionMap caused exception",
                                sqlExc
                            );
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean isInitialized()
    {
        return initialized;
    }
}
