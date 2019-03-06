package com.linbit;

import java.sql.SQLException;
import java.util.Collection;

public interface CollectionDatabaseDriver<PARENT, ELEMENT>
{
    void insert(PARENT parent, ELEMENT value, Collection<ELEMENT> backingCollection) throws SQLException;

    void remove(PARENT parent, ELEMENT value, Collection<ELEMENT> backingCollection) throws SQLException;
}
