package com.linbit.linstor.dbdrivers.interfaces.updater;

import com.linbit.linstor.dbdrivers.DatabaseException;

public interface SingleColumnDatabaseDriver<PARENT, COL_VALUE>
{
    void update(PARENT parent, COL_VALUE oldElement) throws DatabaseException;
}
