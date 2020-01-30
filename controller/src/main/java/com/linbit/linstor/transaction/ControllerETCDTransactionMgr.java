package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.ibm.etcd.api.RequestOp;
import com.ibm.etcd.api.TxnRequest;
import com.ibm.etcd.api.TxnResponse;

public class ControllerETCDTransactionMgr implements TransactionMgrETCD
{
    private final ControllerETCDDatabase etcdDb;
    private final TransactionObjectCollection transactionObjectCollection;
    private final long maxOpsPerTx;
    private EtcdTransaction currentTransaction;

    public ControllerETCDTransactionMgr(ControllerETCDDatabase controllerETCDDatabase, long maxOpsPerTxRef)
    {
        etcdDb = controllerETCDDatabase;
        maxOpsPerTx = maxOpsPerTxRef;
        transactionObjectCollection = new TransactionObjectCollection();
        currentTransaction = createNewEtcdTx();
    }

    private EtcdTransaction createNewEtcdTx()
    {
        currentTransaction = new EtcdTransaction(etcdDb);
        return currentTransaction;
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
        removeDuplucateRequests();

        // TODO check for errors
        TxnResponse txnResponse = EtcdTransaction.requestWithRetry(currentTransaction.etcdTx);

        if (txnResponse.getSucceeded())
        {
            transactionObjectCollection.commitAll();

            clearTransactionObjects();

            currentTransaction = createNewEtcdTx();
        }
        else
        {
            currentTransaction = createNewEtcdTx();
            throw new TransactionException("ETCD commit failed.",
                new LinStorDBRuntimeException(txnResponse.toString())
            );
        }
    }

    private void removeDuplucateRequests()
    {
        // ETCD does not allow duplicate updates for the same key
        TxnRequest request = currentTransaction.etcdTx.asRequest();
        List<RequestOp> successList = new ArrayList<>(request.getSuccessList());
        // we do not use .elseDo(), thus we also only have success entries

        HashMap<String, RequestOp> lastReqMap = new HashMap<>();
        for (RequestOp req : successList)
        {
            String key;
            switch (req.getRequestCase())
            {
                case REQUEST_DELETE_RANGE:
                    key = req.getRequestDeleteRange().getKey().toStringUtf8();
                    break;
                case REQUEST_NOT_SET:
                    key = null;
                    break;
                case REQUEST_PUT:
                    key = req.getRequestPut().getKey().toStringUtf8();
                    break;
                case REQUEST_RANGE:
                    key = req.getRequestRange().getKey().toStringUtf8();
                    break;
                case REQUEST_TXN:
                    key = null;
                    break;
                default:
                    throw new ImplementationError("Unknown ETCD Request case: " + req.getRequestCase());
            }
            if (key != null)
            {
                lastReqMap.put(key, req);
            }
        }
        EtcdTransaction actualTransaction = new EtcdTransaction(etcdDb);
        for (RequestOp req : lastReqMap.values())
        {
            switch (req.getRequestCase())
            {
                case REQUEST_DELETE_RANGE:
                    actualTransaction.etcdTx.delete(req.getRequestDeleteRangeOrBuilder());
                    break;
                case REQUEST_PUT:
                    actualTransaction.etcdTx.put(req.getRequestPutOrBuilder());
                    break;
                case REQUEST_RANGE:
                    actualTransaction.etcdTx.get(req.getRequestRangeOrBuilder());
                    break;
                case REQUEST_NOT_SET:
                case REQUEST_TXN:
                    break;
                default:
                    throw new ImplementationError("Unknown ETCD Request case: " + req.getRequestCase());
            }
        }
        currentTransaction = actualTransaction;
    }

    @Override
    public void rollback() throws TransactionException
    {
        transactionObjectCollection.rollbackAll();

        currentTransaction = createNewEtcdTx();

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
