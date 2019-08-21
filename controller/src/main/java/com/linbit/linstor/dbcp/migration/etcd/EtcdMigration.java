package com.linbit.linstor.dbcp.migration.etcd;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;

import com.ibm.etcd.api.PutRequest;

public class EtcdMigration
{
    protected static PutRequest putReq(String key, String value)
    {
        return PutRequest.newBuilder().setKey(bs(key)).setValue(bs(value)).build();
    }

    protected static String tblKey(GeneratedDatabaseTables.Column tableColumn, String primKey)
    {
        return EtcdUtils.LINSTOR_PREFIX + tableColumn.getTable().getName() + "/" + primKey + "/"
            + tableColumn.getName();
    }
}
