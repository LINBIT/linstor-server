package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;

import java.util.List;

import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

public class ControllerETCDTransactionMgr implements TransactionMgrETCD
{
    private static final Object SYNC_OBJ = new Object();

    private final ControllerETCDDatabase etcdDb;
    private final TransactionObjectCollection transactionObjectCollection;
    private final ControllerETCDRollbackMgr rollbackMgr;

    private EtcdTransaction currentTransaction;

    public ControllerETCDTransactionMgr(
        ControllerETCDDatabase controllerETCDDatabase, int maxOpsPerTxRef, String prefix)
    {
        etcdDb = controllerETCDDatabase;
        transactionObjectCollection = new TransactionObjectCollection();
        currentTransaction = createNewEtcdTx();

        rollbackMgr = new ControllerETCDRollbackMgr(controllerETCDDatabase, maxOpsPerTxRef, prefix);
    }

    private EtcdTransaction createNewEtcdTx()
    {
        return new EtcdTransaction(etcdDb);
    }

    @Override
    public EtcdTransaction getTransaction()
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
        synchronized (SYNC_OBJ)
        {
            /*
             * We need to synchronize to prevent other threads to also start a new rollback entry but also to prevent
             * other to rollback a transaction
             */
            List<FluentTxnOps<?>> txList = rollbackMgr.prepare(currentTransaction);

            boolean allSucceeded = true;
            TxnResponse txnResponse = null;
            for (FluentTxnOps<?> tx : txList)
            {
                txnResponse = EtcdTransaction.requestWithRetry(tx);
                if (!txnResponse.getSucceeded())
                {
                    allSucceeded = false;
                }
            }

            if (allSucceeded)
            {
                transactionObjectCollection.commitAll();

                clearTransactionObjects();

                currentTransaction = createNewEtcdTx();

                rollbackMgr.cleanup();
            }
            else
            {
                currentTransaction = createNewEtcdTx();
                throw new TransactionException(
                    "ETCD commit failed.",
                    new LinStorDBRuntimeException(txnResponse.toString())
                );
            }
        }
    }

    @Override
    public void rollback() throws TransactionException
    {
        synchronized (SYNC_OBJ)
        {
            rollbackMgr.rollback();

            transactionObjectCollection.rollbackAll();

            currentTransaction = createNewEtcdTx();

            clearTransactionObjects();
        }
    }

    /**
     * If the last run of the controller still left some rollback entries - try to
     * perform the rollback that was aborted for some reason in the previous run.
     */
    public void rollbackIfNeeded()
    {
        rollbackMgr.loadRollbackEntries();
        rollbackMgr.rollback();
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
