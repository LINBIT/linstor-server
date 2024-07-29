package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.FluentRequest;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.FluentRangeRequest;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

import static com.ibm.etcd.client.KeyUtils.bs;

public class EtcdTransaction
{
    private final ControllerETCDDatabase etcdDb;
    private final KvClient kvClient;

    FluentTxnOps<?> etcdTx;
    final TreeSet<String> changedKeys;
    final TreeMap<String, Boolean> deletedKeys;

    public EtcdTransaction(ControllerETCDDatabase etcdDbRef)
    {
        etcdDb = etcdDbRef;
        kvClient = etcdDb.getKvClient();
        etcdTx = kvClient.batch();

        changedKeys = new TreeSet<>();
        deletedKeys = new TreeMap<>();
    }

    /*
     * PUT
     */
    public void put(String key, String value)
    {
        etcdTx.put(PutRequest.newBuilder().setKey(bs(key)).setValue(bs(value)).build());
        changedKeys.add(key);
    }

    public void delete(String key)
    {
        delete(key, false);
    }

    /**
     * Deletes the given key.
     * ONLY use the recursive parameter if you are absolutely sure that a recreate-event cannot occur
     */
    public void delete(String key, boolean recursive)
    {
        ByteString bsKey = bs(key);
        DeleteRangeRequest.Builder delBuilder = DeleteRangeRequest.newBuilder().setKey(bsKey);
        if (recursive)
        {
            delBuilder = delBuilder.setRangeEnd(KeyUtils.plusOne(bsKey));
        }
        etcdTx.delete(delBuilder.build());
        deletedKeys.put(key, recursive);
    }


    /*
     * GET
     * All get requests will make their own etcd-transaction as the result is needed immediately
     */
    public TreeMap<String, String> get(String key)
    {
        return get(key, true);
    }

    public TreeMap<String, String> get(String key, boolean recursive)
    {
        FluentRangeRequest req = kvClient.get(bs(key));
        if (recursive)
        {
            req = req.asPrefix();
        }
        RangeResponse rsp = requestWithRetry(req);

        TreeMap<String, String> retMap = new TreeMap<>();
        for (KeyValue keyValue : rsp.getKvsList())
        {
            // final String recKey = keyValue.getKey().toStringUtf8();
            // final String columnName = recKey.substring(recKey.lastIndexOf("/") + 1);
            retMap.put(keyValue.getKey().toStringUtf8(), keyValue.getValue().toStringUtf8());
        }

        return retMap;
    }

    /**
     * Simple wrapper of {@link #get(String)} but only returning the value of the first entry
     * (caution - that is dependent of the underlying map-implementation)
     * If the map was empty, <code>null</code> is returned
     *
     * @param primaryKey
     */
    public @Nullable String getFirstValue(String primaryKey)
    {
        return getFirstValue(primaryKey, null);
    }

    public @Nullable String getFirstValue(String primaryKeyRef, @Nullable String dfltValue)
    {
        Map<String, String> row = get(primaryKeyRef);
        Iterator<String> iterator = row.values().iterator();
        String ret = dfltValue;
        if (iterator.hasNext())
        {
            ret = iterator.next();
        }
        return ret;
    }

    /*
     * INTERNAL methods
     */
    /**
     * Sends the given request with a retry configuration. Waits max 60 seconds for response.
     *
     * @param <RSP>
     * @param req
     *
     * @return
     */
    public static <RSP> RSP requestWithRetry(FluentRequest<?, ?, RSP> req)
    {
        req.backoffRetry();

        RSP ret;
        try
        {
            ret = req.async().get(60, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException exc)
        {
            throw new TransactionException("No connection to ETCD server", exc);
        }
        return ret;
    }

    int getKeyCount()
    {
        return changedKeys.size() + deletedKeys.size();
    }
}
