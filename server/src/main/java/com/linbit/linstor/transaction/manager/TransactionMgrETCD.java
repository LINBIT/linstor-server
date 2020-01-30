package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.transaction.EtcdTransaction;

public interface TransactionMgrETCD extends TransactionMgr
{
    EtcdTransaction getTransaction();
}
