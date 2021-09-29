package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.transaction.K8sCrdTransaction;

public interface TransactionMgrK8sCrd extends TransactionMgr
{
    K8sCrdTransaction getTransaction();
}
