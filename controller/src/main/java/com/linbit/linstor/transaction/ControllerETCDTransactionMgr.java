package com.linbit.linstor.transaction;

import com.ibm.etcd.api.TxnResponse;
import com.linbit.linstor.ControllerETCDDatabase;

import com.ibm.etcd.client.kv.KvClient;
import com.linbit.linstor.LinStorDBRuntimeException;

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

    @Override
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
}
