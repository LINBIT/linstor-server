package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpMapDatabaseDriver;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TransactionMap<PARENT, KEY, VALUE>
    extends AbsTransactionObject implements Map<KEY, VALUE>
{
    private final @Nullable PARENT parent;
    private final MapDatabaseDriver<PARENT, KEY, VALUE> dbDriver;
    private final Map<KEY, VALUE> backingMap;
    private final Map<KEY, VALUE> oldValues;

    public TransactionMap(
        @Nullable PARENT parentRef,
        @Nullable Map<KEY, VALUE> backingMapRef,
        @Nullable MapDatabaseDriver<PARENT, KEY, VALUE> driver,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        parent = parentRef;
        backingMap = backingMapRef == null ? new HashMap<>() : backingMapRef;
        if (driver == null)
        {
            dbDriver = new NoOpMapDatabaseDriver<>();
        }
        else
        {
            if (parentRef == null)
        {
                throw new ImplementationError("Parent must not be null when using a database driver!");
            }
            dbDriver = driver;
        }

        oldValues = new HashMap<>();
    }

    @Override
    public void postSetConnection(TransactionMgr transMgrRef)
    {
        // forward transaction manager on keys
        backingMap.keySet()
            .stream()
            .filter(TransactionObject.class::isInstance)
            .forEach(to -> ((TransactionObject) to).setConnection(transMgrRef));

        // forward transaction manager on values
        backingMap.values()
            .stream()
            .filter(TransactionObject.class::isInstance)
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
                backingMap.remove(key);
            }
            else
            {
                backingMap.put(key, value);
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
        return backingMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        return backingMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return backingMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return backingMap.containsValue(value);
    }

    @Override
    public VALUE get(Object key)
    {
        return backingMap.get(key);
    }

    @Override
    public VALUE put(KEY key, VALUE value)
    {
        VALUE oldValue = backingMap.put(key, value);
        // value must be put into the backing map before (possibly) calling the DB driver
        cache(key, value, oldValue);
        return oldValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public VALUE remove(Object key)
    {
        VALUE oldValue = backingMap.remove(key);
        // value must be removed from the backing map before (possibly) calling the DB driver
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
        HashMap<KEY, VALUE> copy = new HashMap<>(backingMap);
        backingMap.clear();
        // backing map must be cleared before (possibly) calling the database driver
        for (Entry<KEY, VALUE> entry : copy.entrySet())
        {
            cache(entry.getKey(), null, entry.getValue());
        }
    }

    @Override
    public Set<KEY> keySet()
    {
        return Collections.unmodifiableSet(backingMap.keySet());
    }

    @Override
    public Collection<VALUE> values()
    {
        return Collections.unmodifiableCollection(backingMap.values());
    }

    @Override
    public Set<Entry<KEY, VALUE>> entrySet()
    {
        return Collections.unmodifiableSet(backingMap.entrySet());
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
                    dbDriver.insert(parent, backingMap, key, value);
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
                        dbDriver.delete(parent, backingMap, key, oldValue);
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
                        dbDriver.update(parent, backingMap, key, oldValue, value);
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
                eq = objRef.equals(backingMap);
            }
        }

        return eq;
    }

    @Override
    public int hashCode()
    {
        return backingMap.hashCode();
    }

    @Override
    public String toString()
    {
        return "TransactionMap " + backingMap.toString();
    }
}
