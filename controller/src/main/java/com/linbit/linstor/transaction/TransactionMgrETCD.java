package com.linbit.linstor.transaction;

import com.ibm.etcd.client.kv.KvClient;

public interface TransactionMgrETCD extends TransactionMgr
{
    KvClient getClient();

    KvClient.FluentTxnOps<?> getTransaction();
}
