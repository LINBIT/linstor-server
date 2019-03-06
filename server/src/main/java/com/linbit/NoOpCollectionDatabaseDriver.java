package com.linbit;

import java.util.Collection;

public class NoOpCollectionDatabaseDriver<PARENT, VALUE> implements CollectionDatabaseDriver<PARENT, VALUE>
{
    @Override
    public void insert(PARENT parent, VALUE element, Collection<VALUE> backingCollection)
    {
        // no-op
    }

    @Override
    public void remove(PARENT parent, VALUE element, Collection<VALUE> backingCollection)
    {
        // no-op
    }
}
