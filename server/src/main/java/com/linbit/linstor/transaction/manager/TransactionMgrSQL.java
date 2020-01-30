package com.linbit.linstor.transaction.manager;

import java.sql.Connection;

public interface TransactionMgrSQL extends TransactionMgr
{
    Connection getConnection();
}
