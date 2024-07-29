package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpCollectionDatabaseDriver;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

public class TransactionList<PARENT, VALUE>
    extends AbsTransactionObject implements List<VALUE>
{
    private final @Nullable PARENT parent;
    private final CollectionDatabaseDriver<PARENT, VALUE> dbDriver;
    private final List<VALUE> backingList;
    private final List<VALUE> oldValues;
    private final List<VALUE> immutableBackingList;
    private volatile boolean isDirty;

    public TransactionList(
        @Nullable PARENT parentRef,
        List<VALUE> backingListRef,
        @Nullable CollectionDatabaseDriver<PARENT, VALUE> dbDriverRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        parent = parentRef;
        if (dbDriverRef == null)
        {
            dbDriver = new NoOpCollectionDatabaseDriver<>();
        }
        else
        {
            if (parentRef == null)
            {
                throw new ImplementationError("Parent must not be null when using a database driver!");
            }
            dbDriver = dbDriverRef;
        }
        backingList = backingListRef == null ? new ArrayList<>() : backingListRef;
        oldValues = new ArrayList<>();
        immutableBackingList = Collections.unmodifiableList(backingList);
    }

    @Override
    protected void postSetConnection(@Nullable TransactionMgr transMgrRef)
    {
        // forward transaction manager to values
        for (VALUE val : backingList)
        {
            if (val instanceof TransactionObject)
            {
                ((TransactionObject) val).setConnection(transMgrRef);
            }
        }
    }

    @Override
    public boolean isDirty()
    {
        return isDirty;
    }

    @Override
    protected void commitImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("commit"));
        synchronized (oldValues)
        {
            oldValues.clear();
            isDirty = false;
        }
    }

    @Override
    protected void rollbackImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
        synchronized (oldValues)
        {
            backingList.clear();
            backingList.addAll(oldValues);
            oldValues.clear();
            isDirty = false;
        }
    }

    @Override
    public int size()
    {
        return backingList.size();
    }

    @Override
    public boolean isEmpty()
    {
        return backingList.isEmpty();
    }

    @Override
    public boolean contains(Object obj)
    {
        return backingList.contains(obj);
    }

    @Override
    public Iterator<VALUE> iterator()
    {
        return immutableBackingList.iterator();
    }

    @Override
    public Object[] toArray()
    {
        Object[] array = backingList.toArray();
        return Arrays.copyOf(array, array.length);
    }

    @Override
    public <T> T[] toArray(T[] arr)
    {
        return backingList.toArray(arr);
    }

    @Override
    public boolean add(VALUE val)
    {
        markDirty();
        boolean add = backingList.add(val);
        if (add)
        {
            dbInsert(val);
        }
        return add;
    }

    @Override
    public boolean remove(Object obj)
    {
        markDirty();
        boolean ret = backingList.remove(obj);
        if (ret) // also prevents class cast exception
        {
            dbRemove((VALUE) obj);
        }
        return ret;
    }

    @Override
    public boolean containsAll(Collection<?> coll)
    {
        markDirty();
        return backingList.containsAll(coll);
    }

    @Override
    public boolean addAll(Collection<? extends VALUE> coll)
    {
        markDirty();
        Objects.requireNonNull(coll);

        boolean modified = false;
        for (VALUE val : coll)
        {
            modified |= add(val);
        }
        return modified;
    }

    @Override
    public boolean addAll(int index, Collection<? extends VALUE> coll)
    {
        markDirty();
        Objects.requireNonNull(coll);

        for (VALUE val : coll)
        {
            add(index, val);
        }
        return !coll.isEmpty();
    }

    @Override
    public boolean removeAll(Collection<?> coll)
    {
        markDirty();
        Objects.requireNonNull(coll);

        boolean modified = false;
        for (Object val : coll)
        {
            modified |= remove(val);
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> coll)
    {
        markDirty();
        ArrayList<VALUE> toRemove = new ArrayList<>();
        // prevent concurrentModificationException
        for (VALUE val : backingList)
        {
            if (!coll.contains(val))
            {
                toRemove.add(val);
            }
        }

        for (VALUE val : toRemove)
        {
            remove(val);
        }

        return !toRemove.isEmpty();
    }

    @Override
    public void clear()
    {
        ArrayList<VALUE> copy = new ArrayList<>(backingList);
        for (VALUE val : copy)
        {
            remove(val);
        }
    }

    @Override
    public VALUE get(int index)
    {
        return backingList.get(index);
    }

    @Override
    public VALUE set(int index, VALUE element)
    {
        markDirty();
        VALUE set = backingList.set(index, element);
        if (set != null)
        {
            dbRemove(set);
        }
        dbInsert(element);
        return set;
    }

    @Override
    public void add(int index, VALUE element)
    {
        markDirty();
        backingList.add(index, element);
        dbInsert(element);
    }

    @Override
    public VALUE remove(int index)
    {
        markDirty();
        VALUE remove = backingList.remove(index);
        if (remove != null)
        {
            dbRemove(remove);
        }
        return remove;
    }

    @Override
    public int indexOf(Object obj)
    {
        return backingList.indexOf(obj);
    }

    @Override
    public int lastIndexOf(Object obj)
    {
        return backingList.lastIndexOf(obj);
    }

    @Override
    public ListIterator<VALUE> listIterator()
    {
        return immutableBackingList.listIterator();
    }

    @Override
    public ListIterator<VALUE> listIterator(int index)
    {
        return immutableBackingList.listIterator(index);
    }

    @Override
    public List<VALUE> subList(int fromIndex, int toIndex)
    {
        return immutableBackingList.subList(fromIndex, toIndex);
    }

    @SuppressWarnings("null")
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
                eq = objRef.equals(backingList);
            }
        }

        return eq;
    }

    @Override
    public int hashCode()
    {
        return backingList.hashCode();
    }

    private void markDirty()
    {
        if (!isDirty)
        {
            synchronized (oldValues)
            {
                if (!isDirty)
                {
                    oldValues.addAll(backingList);
                    activateTransMgr();
                    isDirty = true;
                }
            }
        }
    }

    private void dbInsert(VALUE val)
    {
        try
        {
            dbDriver.insert(parent, val, backingList);
        }
        catch (DatabaseException exc)
        {
            throw new LinStorRuntimeException("An SQL exception occurred while adding an element", exc);
        }
    }

    private void dbRemove(VALUE val)
    {
        try
        {
            dbDriver.remove(parent, val, backingList);
        }
        catch (DatabaseException exc)
        {
            throw new LinStorDBRuntimeException("An SQL exception occurred while deleting an element", exc);
        }
    }

    @Override
    public String toString()
    {
        return "TransactionList " + backingList.toString();
    }
}
