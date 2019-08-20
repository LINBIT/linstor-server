package com.linbit.linstor.transaction;

import java.sql.Connection;

public interface TransactionMgrSQL extends TransactionMgr
{
    Connection getConnection();
}
