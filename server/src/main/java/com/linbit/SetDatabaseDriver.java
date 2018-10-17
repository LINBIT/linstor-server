package com.linbit;

public interface SetDatabaseDriver<PARENT, VALUE>
{
    void insert(PARENT parent, VALUE value);

    void remove(PARENT parent, VALUE value);
}
