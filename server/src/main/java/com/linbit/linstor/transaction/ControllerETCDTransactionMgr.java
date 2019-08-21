package com.linbit.linstor.transaction;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.LinStorDBRuntimeException;

import java.util.HashMap;
import java.util.Map;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.FluentRangeRequest;

public class ControllerETCDTransactionMgr implements TransactionMgrETCD
{
    private final ControllerETCDDatabase etcdDb;
    private final TransactionObjectCollection transactionObjectCollection;
    private KvClient.FluentTxnOps<?> currentTransaction;

    public ControllerETCDTransactionMgr(ControllerETCDDatabase controllerETCDDatabase)
    {
        etcdDb = controllerETCDDatabase;
        transactionObjectCollection = new TransactionObjectCollection();
        currentTransaction = getClient().batch();
    }

    /*
     * DO NOT expose this client in the interface.
     * Otherwise it is too easy to create additional etcd-transaction which linstor does not track
     * and thus will never be able to commit / rollback
     */
    public KvClient getClient()
    {
        return etcdDb.getKvClient();
    }

    @Override
    public KvClient.FluentTxnOps<?> getTransaction()
    {
        return currentTransaction;
    }

    @Override
    public void register(TransactionObject transObj)
    {
        transactionObjectCollection.register(transObj);
    }

    @Override
    public void commit() throws TransactionException
    {
        // TODO check for errors
        TxnResponse txnResponse = currentTransaction.sync();
        if (txnResponse.getSucceeded())
        {
           transactionObjectCollection.commitAll();

            clearTransactionObjects();

            currentTransaction = getClient().batch();
        }
        else
        {
            currentTransaction = getClient().batch();
            throw new TransactionException("ETCD commit failed.",
                new LinStorDBRuntimeException(txnResponse.toString())
            );
        }
    }

    @Override
    public void rollback() throws TransactionException
    {
        transactionObjectCollection.rollbackAll();

        currentTransaction = getClient().batch();

        clearTransactionObjects();
    }

    @Override
    public void clearTransactionObjects()
    {
        transactionObjectCollection.clearAll();
    }

    @Override
    public boolean isDirty()
    {
        return transactionObjectCollection.areAnyDirty();
    }

    @Override
    public int sizeObjects()
    {
        return transactionObjectCollection.sizeObjects();
    }

    @Override
    public Map<String, String> readTable(String keyRef, boolean recursiveRef)
    {
        /*
         * mostly copied from EtcdUtils which is in the controller server
         * TODO: merge this method with EtcdUtils.getTableRow once we fixed the project-setup
         */
        FluentRangeRequest request = getClient().get(bs(keyRef));
        if (recursiveRef)
        {
            request = request.asPrefix();
        }
        RangeResponse rspRow = request.sync();

        HashMap<String, String> rowMap = new HashMap<>();
        for (KeyValue keyValue : rspRow.getKvsList())
        {
            final String recKey = keyValue.getKey().toStringUtf8();
            final String columnName = recKey.substring(recKey.lastIndexOf("/") + 1);
            rowMap.put(columnName, keyValue.getValue().toStringUtf8());
        }

        return rowMap;
    }
}
