package com.linbit;

public interface CollectionDatabaseDriver<PARENT, ELEMENT>
{
    void insert(PARENT parent, ELEMENT value);

    void remove(PARENT parent, ELEMENT value);
}
