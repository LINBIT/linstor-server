package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerETCDDatabase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.api.RangeRequest.Builder;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.RequestOp;
import com.ibm.etcd.api.ResponseOp;
import com.ibm.etcd.api.ResponseOp.ResponseCase;
import com.ibm.etcd.api.TxnRequest;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

public class ControllerETCDRollbackMgr
{
    private final String namespaceRollback;
    private final String namespaceRollbackUpdate;
    private final String namespaceRollbackDel;
    private final String namespaceRollbackStatus;
    private static final String VALUE_STATUS_READY = "ready";
    private static final String VALUE_DELETE_DUMMY_STR = ":deleteMe";
    private static final ByteString VALUE_DELETE_DUMMY_BS = KeyUtils.bs(VALUE_DELETE_DUMMY_STR);

    private final int maxOpsPerTx;
    private final ControllerETCDDatabase etcdDb;

    private final Map<String, String> currentRollbackMap;

    public ControllerETCDRollbackMgr(ControllerETCDDatabase controllerETCDDatabaseRef, int maxOpsPerTxRef, final String prefix)
    {
        etcdDb = controllerETCDDatabaseRef;
        maxOpsPerTx = maxOpsPerTxRef;

        currentRollbackMap = new TreeMap<>();
        namespaceRollback = prefix + "ROLLBACK/";
        namespaceRollbackUpdate = namespaceRollback + "UPDATE/";
        namespaceRollbackDel = namespaceRollback + "DELETE/";
        namespaceRollbackStatus = namespaceRollback + "STATUS";
    }

    /**
     * Removes duplicate requests to the same key (ETCD does not like those) and performs
     * preparations for a possible rollback action.
     *
     * @param currentTransactionRef
     *
     * @return
     */
    public List<FluentTxnOps<?>> prepare(EtcdTransaction currentTransactionRef)
    {
        List<FluentTxnOps<?>> txList = removeDuplucateRequests(currentTransactionRef);

        getRollbackMap(currentTransactionRef);
        writeRollbackEntries();

        return txList;
    }

    public void cleanup()
    {
        FluentTxnOps<?> tmpTx = etcdDb.getKvClient().batch();
        ByteString bsKey = KeyUtils.bs(namespaceRollback);
        tmpTx.delete(
            DeleteRangeRequest.newBuilder()
                .setKey(bsKey)
                .setRangeEnd(KeyUtils.plusOne(bsKey))
                .build()
        );
        flush(tmpTx, "Failed to cleanup rollback entries");
        currentRollbackMap.clear();
    }

    public void rollback()
    {
        if (!currentRollbackMap.isEmpty())
        {
            String rollbackStatus = currentRollbackMap.get(namespaceRollbackStatus);
            if (rollbackStatus != null && rollbackStatus.equals(VALUE_STATUS_READY))
            {
                FluentTxnOps<?> tx = etcdDb.getKvClient().batch();
                int ops = 0;
                for (Entry<String, String> entry : currentRollbackMap.entrySet())
                {
                    if (ops == maxOpsPerTx)
                    {
                        flush(tx, "Failed to rollback transaction");
                        tx = etcdDb.getKvClient().batch();
                        ops = 0;
                    }
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if (key.startsWith(namespaceRollbackDel))
                    {
                        key = key.substring(namespaceRollbackDel.length());
                        tx.delete(
                            DeleteRangeRequest.newBuilder()
                                .setKey(KeyUtils.bs(key))
                                .build()
                        );
                    }
                    else
                    if (key.startsWith(namespaceRollbackUpdate))
                    {
                        key = key.substring(namespaceRollbackUpdate.length());
                        tx.put(
                            PutRequest.newBuilder()
                                .setKey(KeyUtils.bs(key))
                                .setValue(KeyUtils.bs(value))
                                .build()
                        );
                    }
                    // else only other key is STATUS, just ignore that, will be deleted in cleanup()
                    ops++;
                }
                if (ops > 0)
                {
                    flush(tx, "Failed to rollback transaction");
                }
            }
            cleanup();
        }
    }

    public void loadRollbackEntries()
    {
        FluentTxnOps<?> tx = etcdDb.getKvClient().batch();
        tx.get(
            RangeRequest.newBuilder()
                .setKey(KeyUtils.bs(namespaceRollback))
                .setRangeEnd(
                    KeyUtils.plusOne(
                        KeyUtils.bs(
                            namespaceRollback
                        )
                    )
                )
                .build()
        );
        processGetRequests(tx);
    }

    private List<FluentTxnOps<?>> removeDuplucateRequests(EtcdTransaction currentTransaction)
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

        int ops = 0;
        FluentTxnOps<?> curTx = etcdDb.getKvClient().batch();
        List<FluentTxnOps<?>> ret = new ArrayList<>();
        ret.add(curTx);
        for (Entry<String, RequestOp> reqEntry : lastReqMap.entrySet())
        {
            RequestOp req = reqEntry.getValue();

            if (ops == maxOpsPerTx)
            {
                curTx = etcdDb.getKvClient().batch();
                ret.add(curTx);

                ops = 0;
            }
            switch (req.getRequestCase())
            {
                case REQUEST_DELETE_RANGE:
                    curTx.delete(req.getRequestDeleteRangeOrBuilder());
                    break;
                case REQUEST_PUT:
                    curTx.put(req.getRequestPutOrBuilder());
                    break;
                case REQUEST_RANGE:
                    curTx.get(req.getRequestRangeOrBuilder());
                    break;
                case REQUEST_NOT_SET:
                case REQUEST_TXN:
                    break;
                default:
                    throw new ImplementationError("Unknown ETCD Request case: " + req.getRequestCase());
            }
            ops++;
        }

        return ret;
    }

    private void getRollbackMap(EtcdTransaction currentTransaction)
    {
        if (maxOpsPerTx < currentTransaction.getKeyCount())
        {
            int ops = 0;
            FluentTxnOps<?> tmpTx = etcdDb.getKvClient().batch();
            for (String key : currentTransaction.changedKeys)
            {
                if (ops >= maxOpsPerTx)
                {
                    processGetRequests(tmpTx);
                    tmpTx = etcdDb.getKvClient().batch();
                    ops = 0;
                }

                tmpTx.get(
                    RangeRequest.newBuilder()
                        .setKey(KeyUtils.bs(key))
                        .build()
                );
                currentRollbackMap.put(key, null);
                ops++;
            }

            for (Entry<String, Boolean> delKey : currentTransaction.deletedKeys.entrySet())
            {
                String key = delKey.getKey();

                if (ops >= maxOpsPerTx)
                {
                    processGetRequests(tmpTx);
                    tmpTx = etcdDb.getKvClient().batch();
                    ops = 0;
                }

                Builder getBuilder = RangeRequest.newBuilder();
                ByteString keyBs = KeyUtils.bs(key);
                getBuilder = getBuilder.setKey(keyBs);
                boolean recursive = delKey.getValue() == null ? false : delKey.getValue();
                if (recursive)
                {
                    getBuilder = getBuilder.setRangeEnd(KeyUtils.plusOne(keyBs));
                }
                currentRollbackMap.put(key, null);
                tmpTx.get(getBuilder.build());
                ops++;
            }
        }
    }

    private void processGetRequests(FluentTxnOps<?> txRef)
    {
        TxnResponse resp = EtcdTransaction.requestWithRetry(txRef);
        if (!resp.getSucceeded())
        {
            throw new TransactionException("Failed to query data from ETCD server", null);
        }
        for (ResponseOp responseOp : resp.getResponsesList())
        {
            if (!responseOp.getResponseCase().equals(ResponseCase.RESPONSE_RANGE))
            {
                throw new TransactionException("Unexpected response case: " + responseOp.getResponseCase(), null);
            }

            RangeResponse rangeResponse = responseOp.getResponseRange();

            for (KeyValue kv : rangeResponse.getKvsList())
            {
                currentRollbackMap.put(kv.getKey().toStringUtf8(), kv.getValue().toStringUtf8());
            }
        }
    }

    private void writeRollbackEntries()
    {
        if (!currentRollbackMap.isEmpty())
        {
            Consumer<FluentTxnOps<?>> writeOrDie = tx ->
            {
                TxnResponse response = EtcdTransaction.requestWithRetry(tx);
                if (!response.getSucceeded())
                {
                    cleanup();
                    throw new TransactionException("Failed to create rollback entries", null);
                }
            };

            FluentTxnOps<?> tmpTx = etcdDb.getKvClient().batch();
            int ops = 0;
            boolean writtenRollbackEntries = false;
            for (Entry<String, String> entry : currentRollbackMap.entrySet())
            {
                if (ops == maxOpsPerTx)
                {
                    writeOrDie.accept(tmpTx);
                    writtenRollbackEntries = true;
                    tmpTx = etcdDb.getKvClient().batch();
                    ops = 0;
                }

                String valueToRollback = entry.getValue();
                String key = entry.getKey();
                if (valueToRollback == null)
                {
                    tmpTx.put(
                        PutRequest.newBuilder()
                            .setKey(KeyUtils.bs(namespaceRollbackDel + key))
                            .setValue(VALUE_DELETE_DUMMY_BS)
                            .build()
                    );
                }
                else
                {
                    tmpTx.put(
                        PutRequest.newBuilder()
                            .setKey(KeyUtils.bs(namespaceRollbackUpdate + key))
                            .setValue(KeyUtils.bs(valueToRollback))
                            .build()
                    );
                }
                ops++;
            }
            if (ops > 0 || writtenRollbackEntries)
            {
                if (ops == maxOpsPerTx)
                {
                    writeOrDie.accept(tmpTx);
                    tmpTx = etcdDb.getKvClient().batch();
                }
                tmpTx.put(
                    PutRequest.newBuilder()
                        .setKey(KeyUtils.bs(namespaceRollbackStatus))
                        .setValue(KeyUtils.bs(VALUE_STATUS_READY))
                        .build()
                );
                writeOrDie.accept(tmpTx);
            }
        }
    }

    private void flush(FluentTxnOps<?> tx, String excMsg)
    {
        TxnResponse response = EtcdTransaction.requestWithRetry(tx);
        if (!response.getSucceeded())
        {
            throw new TransactionException(excMsg, null);
        }
    }
}
