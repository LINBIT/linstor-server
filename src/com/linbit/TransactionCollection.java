package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import com.linbit.drbdmanage.DrbdSqlRuntimeException;

public class TransactionCollection<T> implements TransactionObject, Collection<T>
{
    private Collection<T> data;

    protected Collection<T> inserted;
    protected Collection<T> deleted;

    private ObjectDatabaseDriver<T> dbDriver;

    private Connection con;

    private boolean initialized = false;

    public TransactionCollection(Collection<T> data, ObjectDatabaseDriver<T> driver)
    {
        this.data = data;

        if (driver == null)
        {
            dbDriver = new NoOpObjectDatabaseDriver<T>();
        }
        else
        {
            dbDriver = driver;
        }

        inserted = new ArrayList<>();
        deleted = new ArrayList<>();
    }

    @Override
    public void initialized()
    {
        initialized = true;
    }

    @Override
    public boolean isDirty()
    {
        return !inserted.isEmpty() || !deleted.isEmpty();
    }

    @Override
    public void setConnection(TransactionMgr transMgr)
    {
        if (transMgr != null)
        {
            transMgr.register(this);
            con = transMgr.dbCon;
        }
        else
        {
            con = null;
        }
    }

    @Override
    public void commit()
    {
        // data already up to date - just clear the rollback-caches
        inserted.clear();
        deleted.clear();
    }

    @Override
    public void rollback()
    {
        // apply the caches
        data.removeAll(inserted);
        data.addAll(deleted);

        // data should be now rolled back - clear the caches
        inserted.clear();
        deleted.clear();
    }

    @Override
    public int size()
    {
        return data.size();
    }

    @Override
    public boolean isEmpty()
    {
        return data.isEmpty();
    }

    @Override
    public boolean contains(Object o)
    {
        return data.contains(o);
    }

    @Override
    public Iterator<T> iterator()
    {
        return Collections.unmodifiableCollection(data).iterator();
    }

    @Override
    public Object[] toArray()
    {
        return data.toArray();
    }

    @Override
    public <U> U[] toArray(U[] array)
    {
        return data.toArray(array);
    }

    @Override
    public boolean add(T element)
    {
        boolean changed = data.add(element);
        if (initialized && changed)
        {
            inserted.add(element);
            if (con != null)
            {
                try
                {
                    dbDriver.insert(con, element);
                }
                catch (SQLException sqlExc)
                {
                    throw new DrbdSqlRuntimeException("Adding an element to TransactionCollection failed", sqlExc);
                }
            }
        }
        return changed;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object element)
    {
        // delete.add and dbDriver.delete are bound to type T - element is an
        // Object, which could be unsafe to "just cast it"

        boolean remove = data.remove(element);
        // however, if data (which is also of type T) contained the element,
        // the element itself has to be of type T (otherwise it couldn't be
        // added to data in the first place)
        if (initialized && remove)
        {
            // thus, it is now safe to cast element to T
            deleted.add((T) element);
            if (con != null)
            {
                try
                {
                    dbDriver.delete(con, (T) element);
                }
                catch (SQLException sqlExc)
                {
                    throw new DrbdSqlRuntimeException("Removing an element to TransactionCollection failed", sqlExc);
                }
            }
        }
        return remove;
    }

    @Override
    public boolean containsAll(Collection<?> collection)
    {
        return data.containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends T> collection)
    {
        boolean changed = false;
        for (T elem : collection)
        {
            if (data.add(elem))
            {
                if (initialized)
                {
                    if (con != null)
                    {
                        try
                        {
                            dbDriver.insert(con, elem);
                        }
                        catch (SQLException sqlExc)
                        {
                            throw new DrbdSqlRuntimeException("Adding an element to TransactionCollection failed", sqlExc);
                        }
                    }
                    inserted.add(elem);
                }
                changed = true;
            }
        }
        return changed;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean removeAll(Collection<?> collection)
    {
        boolean changed = false;
        for (Object elem : collection)
        {
            if (data.remove(elem))
            {
                if (initialized)
                {
                    if (con != null)
                    {
                        try
                        {
                            dbDriver.delete(con, (T) elem);
                        }
                        catch (SQLException sqlExc)
                        {
                            throw new DrbdSqlRuntimeException("Removing an element to TransactionCollection failed", sqlExc);
                        }
                    }
                    deleted.add((T) elem);
                }
                changed = true;
            }
        }
        return changed;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean retainAll(Collection<?> collection)
    {
        boolean changed = false;
        for (Object elem : collection)
        {
            if (data.remove(elem))
            {
                if (initialized)
                {
                    if (con != null)
                    {
                        try
                        {
                            dbDriver.delete(con, (T) elem);
                        }
                        catch (SQLException sqlExc)
                        {
                            throw new DrbdSqlRuntimeException("Removing an element to TransactionCollection failed", sqlExc);
                        }
                    }
                    deleted.add((T) elem);
                }
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public void clear()
    {
        if (initialized)
        {
            if (con != null)
            {
                for (T elem : data)
                {
                    try
                    {
                        dbDriver.delete(con, elem);
                    }
                    catch (SQLException sqlExc)
                    {
                        throw new DrbdSqlRuntimeException("Removing an element to TransactionCollection failed", sqlExc);
                    }
                }
            }
            deleted.addAll(data);
        }
        data.clear();
    }
}
