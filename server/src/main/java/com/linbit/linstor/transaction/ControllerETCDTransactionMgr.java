package com.linbit.linstor.transaction;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.LinStorDBRuntimeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.RequestOp;
import com.ibm.etcd.api.TxnRequest;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.FluentRangeRequest;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

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
        removeDuplucateRequests();

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

    private void removeDuplucateRequests()
    {
        // ETCD does not allow duplicate updates for the same key
        TxnRequest request = currentTransaction.asRequest();
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
        FluentTxnOps<?> actualTransaction = getClient().batch();
        for (RequestOp req : lastReqMap.values())
        {
            switch (req.getRequestCase())
            {
                case REQUEST_DELETE_RANGE:
                    actualTransaction.delete(req.getRequestDeleteRangeOrBuilder());
                    break;
                case REQUEST_PUT:
                    actualTransaction.put(req.getRequestPutOrBuilder());
                    break;
                case REQUEST_RANGE:
                    actualTransaction.get(req.getRequestRangeOrBuilder());
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
    public TreeMap<String, String> readTable(String keyRef, boolean recursiveRef)
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

        TreeMap<String, String> rowMap = new TreeMap<>();
        for (KeyValue keyValue : rspRow.getKvsList())
        {
            // final String recKey = keyValue.getKey().toStringUtf8();
            // final String columnName = recKey.substring(recKey.lastIndexOf("/") + 1);
            rowMap.put(keyValue.getKey().toStringUtf8(), keyValue.getValue().toStringUtf8());
        }

        return rowMap;
    }
}
