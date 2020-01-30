package com.linbit.linstor.dbdrivers.etcd;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.transaction.EtcdTransaction;
import com.linbit.linstor.transaction.TransactionMgrETCD;

import javax.inject.Provider;

import java.util.Map;

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
    public FluentLinstorTransaction namespace(DatabaseTable table, String... pks)
    {
        return namespace(EtcdUtils.buildKey(table, pks));
    }

    /**
     * Starts a new {@link FluentLinstorTransaction} with the given base key
     *
     * @param baseKeyRef
     */
    public FluentLinstorTransaction namespace(String baseKey)
    {
        return new FluentLinstorTransaction(getTransaction(), baseKey);
    }

    public static class FluentLinstorTransaction
    {
        private final TransactionMgrETCD transactionMgrETCD;
        private final EtcdTransaction tx;
        private String currentBaseKey;

        public FluentLinstorTransaction(TransactionMgrETCD transactionMgrETCDRef, String baseKeyRef)
        {
            transactionMgrETCD = transactionMgrETCDRef;
            tx = transactionMgrETCDRef.getTransaction();
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
            tx.put(currentBaseKey + key, valueRef);
            return this;
        }

        public FluentLinstorTransaction delete(boolean recursive)
        {
            tx.delete(currentBaseKey, recursive);
            return this;
        }

        public Map<String, String> get(boolean recursive) throws DatabaseException
        {
            try
            {
                return tx.get(currentBaseKey, recursive);
            }
            catch (io.grpc.StatusRuntimeException grpcExc)
            {
                throw new DatabaseException(grpcExc);
            }
        }
    }
}
