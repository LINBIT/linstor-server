package com.linbit.linstor.transaction;

public interface TransactionMgrETCD extends TransactionMgr
{
    EtcdTransaction getTransaction();
}
