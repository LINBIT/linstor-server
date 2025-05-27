package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpCollectionDatabaseDriver;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class TransactionSet<PARENT, VALUE>
    extends AbsTransactionObject implements Set<VALUE>
{
    private final @Nullable PARENT parent;
    private final CollectionDatabaseDriver<PARENT, VALUE> dbDriver;
    private final Set<VALUE> backingSet;
    private final Set<VALUE> oldValues;
    private boolean isDirty = false;

    public TransactionSet(
        @Nullable PARENT parentRef,
        Set<VALUE> backingSetRef,
        @Nullable CollectionDatabaseDriver<PARENT, VALUE> dbDriverRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        parent = parentRef;
        backingSet = backingSetRef == null ? new HashSet<>() : backingSetRef;
        oldValues = new LinkedHashSet<>();
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
    }

    @Override
    protected void postSetConnection(@Nullable TransactionMgr transMgrRef)
    {
        // forward transaction manager to values
        for (VALUE val : backingSet)
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
        /*
         * DO NOT use
         * return !oldValues.equals(backingSet);
         * or similar, as the oldValues is only populated when markDirty is called.
         * markDirty HAS to be called to activate the transaction manager, which means it is also
         * not necessary to add "oldValues.addAll(backingSet);" in the method commitImpl().
         */
        return isDirty;
    }

    @Override
    protected void commitImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("commit"));
        oldValues.clear();
        isDirty = false;
    }

    @Override
    protected void rollbackImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
        backingSet.clear();
        backingSet.addAll(oldValues);
        oldValues.clear();
        isDirty = false;
    }

    @Override
    public int size()
    {
        return backingSet.size();
    }

    @Override
    public boolean isEmpty()
    {
        return backingSet.isEmpty();
    }

    @Override
    public boolean contains(Object obj)
    {
        return backingSet.contains(obj);
    }

    @Override
    public Iterator<VALUE> iterator()
    {
        // workaround to prevent the call of iterator.remove()
        final Iterator<VALUE> it = backingSet.iterator();
        return new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return it.hasNext();
            }

            @Override
            public VALUE next()
            {
                return it.next();
            }
        };
    }

    @Override
    public Object[] toArray()
    {
        Object[] array = backingSet.toArray();
        return Arrays.copyOf(array, array.length);
    }

    @Override
    public <T> T[] toArray(T[] arr)
    {
        return backingSet.toArray(arr);
    }

    @Override
    public boolean add(VALUE element)
    {
        markDirty();
        boolean ret = backingSet.add(element);
        if (ret)
        {
            try
            {
                dbDriver.insert(parent, element, backingSet);
            }
            catch (DatabaseException exc)
            {
                throw new LinStorDBRuntimeException("A database exception occurred while adding an element", exc);
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object obj)
    {
        markDirty();
        boolean ret = backingSet.remove(obj);
        if (ret) // also prevents class cast exception
        {
            try
            {
                dbDriver.remove(parent, (VALUE) obj, backingSet);
            }
            catch (DatabaseException exc)
            {
                throw new LinStorDBRuntimeException("A database exception occurred while deleting an element", exc);
            }
        }
        return ret;
    }

    private void markDirty()
    {
        if (!isDirty)
        {
            synchronized (oldValues)
            {
                if (!isDirty)
                {
                    oldValues.addAll(backingSet);
                    activateTransMgr();
                    isDirty = true;
                }
            }
        }
    }

    @Override
    public boolean containsAll(Collection<?> coll)
    {
        Objects.requireNonNull(coll);
        return backingSet.containsAll(coll);
    }

    @Override
    public boolean addAll(Collection<? extends VALUE> coll)
    {
        Objects.requireNonNull(coll);

        boolean modified = false;
        for (VALUE val : coll)
        {
            modified |= add(val);
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> coll)
    {
        Objects.requireNonNull(coll);

        HashSet<VALUE> toRemove = new HashSet<>();
        // prevent concurrentModificationException
        for (VALUE val : backingSet)
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
    public boolean removeAll(Collection<?> coll)
    {
        Objects.requireNonNull(coll);

        boolean modified = false;
        for (Object val : coll)
        {
            modified |= remove(val);
        }
        return modified;
    }

    @Override
    public void clear()
    {
        HashSet<VALUE> copy = new HashSet<>(backingSet);
        for (VALUE val : copy)
        {
            remove(val);
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
                eq = objRef.equals(backingSet);
            }
        }

        return eq;
    }

    @Override
    public int hashCode()
    {
        return backingSet.hashCode();
    }

    @Override
    public String toString()
    {
        return "TransactionSet " + backingSet.toString();
    }
}
