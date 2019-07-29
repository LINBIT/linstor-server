package com.linbit;

import com.linbit.linstor.dbdrivers.DatabaseException;

public interface SingleColumnDatabaseDriver<PARENT, COL_VALUE>
{
    void update(PARENT parent, COL_VALUE element) throws DatabaseException;
}
