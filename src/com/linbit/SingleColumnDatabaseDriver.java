package com.linbit;

import java.sql.SQLException;

public interface SingleColumnDatabaseDriver<PARENT, COL_VALUE>
{
    public void update(PARENT parent, COL_VALUE element, TransactionMgr transMgr) throws SQLException;
}
