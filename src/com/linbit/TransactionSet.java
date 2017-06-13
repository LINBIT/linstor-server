package com.linbit;

import java.util.Collection;
import java.util.LinkedHashSet;

public class TransactionSet<T> extends TransactionCollection<T>
{

    public TransactionSet(Collection<T> data, ObjectDatabaseDriver<T> dbDriver)
    {
        super(data, dbDriver);
        inserted = new LinkedHashSet<>();
        deleted = new LinkedHashSet<>();
    }

    @Override
    public boolean addAll(Collection<? extends T> collection)
    {
        boolean changed = false;
        for (T elem : collection)
        {
            if (super.contains(elem))
            {
                changed = true;
                super.add(elem);
            }
        }
        return changed;
    }

    @Override
    public boolean removeAll(Collection<?> collection)
    {
        boolean changed = false;
        for (Object elem : collection)
        {
            if (super.contains(elem))
            {
                changed = true;
                super.remove(elem);
            }
        }
        return changed;
    }

    @Override
    public boolean retainAll(Collection<?> collection)
    {
        boolean changed = false;
        for (Object elem : collection)
        {
            if (!super.contains(elem))
            {
                changed = true;
                super.remove(elem);
            }
        }
        return changed;
    }
}
