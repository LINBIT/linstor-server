package com.linbit.linstor.transaction;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.NoOpCollectionDatabaseDriver;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class TransactionList<PARENT, VALUE extends TransactionObject>
    extends AbsTransactionObject implements List<VALUE>
{
    private final PARENT parent;
    private final CollectionDatabaseDriver<PARENT, VALUE> dbDriver;
    private final List<VALUE> backingList;
    private final List<VALUE> oldValues;
    private final List<VALUE> immutableBackingList;
    private volatile boolean isDirty;

    public TransactionList(
        PARENT parentRef,
        List<VALUE> backingListRef,
        CollectionDatabaseDriver<PARENT, VALUE> dbDriverRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        parent = parentRef;
        dbDriver = dbDriverRef == null ? new NoOpCollectionDatabaseDriver<>() : dbDriverRef;
        backingList = backingListRef == null ? new ArrayList<>() : backingListRef;
        oldValues = new ArrayList<>();
        immutableBackingList = Collections.unmodifiableList(backingList);
    }

    @Override
    protected void postSetConnection(TransactionMgr transMgrRef)
    {
        // forward transaction manager to values
        backingList.forEach(to -> to.setConnection(transMgrRef));
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
        return backingList.add(val);
    }

    @Override
    public boolean remove(Object obj)
    {
        markDirty();
        return backingList.remove(obj);
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
        return backingList.addAll(coll);
    }

    @Override
    public boolean addAll(int index, Collection<? extends VALUE> coll)
    {
        markDirty();
        return backingList.addAll(index, coll);
    }

    @Override
    public boolean removeAll(Collection<?> coll)
    {
        markDirty();
        return backingList.removeAll(coll);
    }

    @Override
    public boolean retainAll(Collection<?> coll)
    {
        markDirty();
        return backingList.retainAll(coll);
    }

    @Override
    public void clear()
    {
        if (!backingList.isEmpty())
        {
            markDirty();
            backingList.clear();
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
        return backingList.set(index, element);
    }

    @Override
    public void add(int index, VALUE element)
    {
        markDirty();
        backingList.add(index, element);
    }

    @Override
    public VALUE remove(int index)
    {
        markDirty();
        return backingList.remove(index);
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

    private void markDirty()
    {
        if (!isDirty)
        {
            synchronized (oldValues)
            {
                if (!isDirty)
                {
                    oldValues.addAll(backingList);
                    isDirty = true;
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return "TransactionList " + backingList.toString();
    }
}
