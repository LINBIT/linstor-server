package com.linbit.linstor.dbdrivers.etcd;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Table;
import com.linbit.linstor.transaction.TransactionMgrETCD;

import javax.inject.Provider;

import java.util.Map;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.kv.KvClient;

public abstract class BaseEtcdDriver
{
    protected final Provider<TransactionMgrETCD> transMgrProvider;

    public BaseEtcdDriver(Provider<TransactionMgrETCD> transMgrProviderRef)
    {
        transMgrProvider = transMgrProviderRef;
    }

    protected TransactionMgrETCD getTransaction()
    {
        return transMgrProvider.get();
    }

    /**
     * Starts a new {@link FluentLinstorTransaction} with the given base key
     *
     * @param baseKeyRef
     */
    protected FluentLinstorTransaction namespace(Table table, String... pks)
    {
        return namespace(EtcdUtils.buildKey(table, pks));
    }

    /**
     * Starts a new {@link FluentLinstorTransaction} with the given base key
     *
     * @param baseKeyRef
     */
    protected FluentLinstorTransaction namespace(String baseKey)
    {
        return new FluentLinstorTransaction(getTransaction(), baseKey);
    }

    public static class FluentLinstorTransaction
    {
        private final TransactionMgrETCD transactionMgrETCD;
        private final KvClient.FluentTxnOps<?> txn;
        private String currentBaseKey;

        public FluentLinstorTransaction(TransactionMgrETCD transactionMgrETCDRef, String baseKeyRef)
        {
            transactionMgrETCD = transactionMgrETCDRef;
            txn = transactionMgrETCDRef.getTransaction();
            currentBaseKey = baseKeyRef;
        }

        public FluentLinstorTransaction put(String baseKeyRef, Column column, String valueRef)
        {
            currentBaseKey = baseKeyRef;
            return put(column, valueRef);
        }

        /**
         * Puts a new column for the previously defined base key.
         *
         * @param column
         * @param valueRef
         */
        public FluentLinstorTransaction put(Column column, String valueRef)
        {
            return put(column.getName(), valueRef);
        }

        /**
         * Puts an arbitrary key for the previously defined base key.
         *
         * @param column
         * @param valueRef
         */
        public FluentLinstorTransaction put(String key, String valueRef)
        {
            txn.put(
                PutRequest.newBuilder()
                    .setKey(bs(currentBaseKey + key))
                    .setValue(bs(valueRef)).build()
            );
            return this;
        }

        public FluentLinstorTransaction delete(boolean recursive)
        {
            ByteString bsKey = bs(currentBaseKey);
            DeleteRangeRequest.Builder deleteBuilder = DeleteRangeRequest.newBuilder().setKey(bsKey);
            if (recursive)
            {
                deleteBuilder = deleteBuilder.setRangeEnd(KeyUtils.plusOne(bsKey));
            }
            txn.delete(deleteBuilder.build());
            return this;
        }

        public Map<String, String> get(boolean recursive)
        {
            return transactionMgrETCD.readTable(currentBaseKey, recursive);
        }
    }
}
