package com.linbit.linstor.transaction;

public interface TransactionMgrGenerator
{
    TransactionMgr startTransaction()
        throws TransactionException;
}
