package com.linbit.linstor.dbdrivers.etcd;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.transaction.EtcdTransaction;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;

import javax.inject.Provider;

import java.util.Map;

public abstract class BaseEtcdDriver
{
    protected static final String DUMMY_NULL_VALUE = ":null";

    protected final Provider<TransactionMgrETCD> transMgrProvider;

    public BaseEtcdDriver(Provider<TransactionMgrETCD> transMgrProviderRef)
    {
        transMgrProvider = transMgrProviderRef;
    }

    protected TransactionMgrETCD getTransaction()
    {
        return transMgrProvider.get();
    }

    public String buildPrefixedRscLayerIdKey(DatabaseTable table, int rscLayerId)
    {
        String etcdKey = EtcdUtils.buildKey(table, Integer.toString(rscLayerId));
        // we did not specify the full PK, only the rscLayerId. PK should be something like
        // <rscId>:<vlmNr>
        // however, the returned etcdKey is something like "/LINSTOR/<table>/<rscId>/"
        // as we are using the --prefix, we need to cut away the last '/' in order to get all
        // volumes of this <rscId>
        etcdKey = etcdKey.substring(0, etcdKey.length() - EtcdUtils.PATH_DELIMITER.length());

        // However, if we have i.e. a rscId of 10, we have to make sure to only match volumes starting with 10 only
        // and not also match 10* (i.e. 100, 101, ...). Since we cut the last "/" from the initial
        // "/LINSTOR/<table>/<rscId>/" in the previous step to get
        // "/LINSTOR/<table>/<rscId>", we now add the PK delimiter to limit the resource-ids properly:
        // "/LINSTOR/<table>/<rscId>:"
        etcdKey += EtcdUtils.PK_DELIMITER;
        return etcdKey;
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

        public FluentLinstorTransaction delete()
        {
            tx.delete(currentBaseKey);
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
