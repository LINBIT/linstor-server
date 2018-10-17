package com.linbit;

public class NoOpSetDatabaseDriver<PARENT, VALUE> implements SetDatabaseDriver<PARENT, VALUE>
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
