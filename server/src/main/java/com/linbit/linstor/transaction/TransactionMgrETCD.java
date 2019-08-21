package com.linbit.linstor.transaction;

import java.util.Map;

import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

public interface TransactionMgrETCD extends TransactionMgr
{
    KvClient.FluentTxnOps<?> getTransaction();

    /**
     * Reads the requested key in a new transaction
     * NOTE: this is NOT dont in the same {@link FluentTxnOps} as returned by
     * {@link #getTransaction()} as for getting the values we need to call{@link FluentTxnOps#sync()}
     * which "commits" the transaction
     *
     * @param key
     * @param recursive
     * @return
     */
    Map<String, String> readTable(String key, boolean recursive);
}
