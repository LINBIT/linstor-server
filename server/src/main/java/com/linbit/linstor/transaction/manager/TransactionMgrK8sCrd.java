package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.transaction.K8sCrdTransaction;

public interface TransactionMgrK8sCrd<T extends RollbackCrd> extends TransactionMgr
{
    K8sCrdTransaction<T> getTransaction();
}
