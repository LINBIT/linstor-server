package com.linbit;

public class NoOpCollectionDatabaseDriver<PARENT, VALUE> implements CollectionDatabaseDriver<PARENT, VALUE>
{
    @Override
    public void insert(PARENT parent, VALUE element)
    {
        // no-op
    }

    @Override
    public void remove(PARENT parent, VALUE element)
    {
        // no-op
    }
}
