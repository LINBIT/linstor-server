package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.transaction.TransactionException;

public interface TransactionMgrGenerator
{
    TransactionMgr startTransaction()
        throws TransactionException;
}
