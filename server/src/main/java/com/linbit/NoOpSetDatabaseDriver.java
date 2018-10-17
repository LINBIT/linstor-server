package com.linbit;

public class NoOpSetDatabaseDriver<T> implements SetDatabaseDriver<T>
{
    @Override
    public void insert(T element)
    {
        // no-op
    }

    @Override
    public void remove(T element)
    {
        // no-op
    }
}
