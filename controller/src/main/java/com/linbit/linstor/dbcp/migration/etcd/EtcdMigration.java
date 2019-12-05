package com.linbit.linstor.dbcp.migration.etcd;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.client.KeyUtils;

public abstract class EtcdMigration
{
    public static RangeRequest getReq(String key, boolean recursive)
    {
        ByteString bsKey = bs(key);
        RangeRequest.Builder requestBuilder = RangeRequest.newBuilder().setKey(bsKey);
        if (recursive)
        {
            requestBuilder = requestBuilder.setRangeEnd(KeyUtils.plusOne(bsKey));
        }
        return requestBuilder.build();
    }

    public static PutRequest putReq(String key, String value)
    {
        return PutRequest.newBuilder().setKey(bs(key)).setValue(bs(value)).build();
    }

    public static DeleteRangeRequest delReq(String key, boolean recursive)
    {
        ByteString bsKey = bs(key);
        DeleteRangeRequest.Builder delBuilder = DeleteRangeRequest.newBuilder().setKey(bsKey);
        if (recursive)
        {
            delBuilder = delBuilder.setRangeEnd(KeyUtils.plusOne(bsKey));
        }
        return delBuilder.build();
    }

    public static String tblKey(GeneratedDatabaseTables.Column tableColumn, String primKey)
    {
        return EtcdUtils.buildKey(tableColumn, primKey);
    }
}
