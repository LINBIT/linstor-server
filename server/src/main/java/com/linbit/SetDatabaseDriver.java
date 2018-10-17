package com.linbit;

public interface SetDatabaseDriver<T>
{
    void insert(T element);

    void remove(T element);
}
