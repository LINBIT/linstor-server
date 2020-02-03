package com.linbit.linstor.dbdrivers.interfaces.updater;

import com.linbit.linstor.dbdrivers.DatabaseException;

import java.util.Collection;

public interface CollectionDatabaseDriver<PARENT, ELEMENT>
{
    void insert(PARENT parent, ELEMENT value, Collection<ELEMENT> backingCollection) throws DatabaseException;

    void remove(PARENT parent, ELEMENT value, Collection<ELEMENT> backingCollection) throws DatabaseException;
}
