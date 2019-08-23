package com.linbit.linstor.dbcp.migration.etcd;

import com.ibm.etcd.api.PutRequest;
import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;

import static com.ibm.etcd.client.KeyUtils.bs;

public class EtcdMigration
{
    protected static PutRequest putReq(String key, String value)
    {
        return PutRequest.newBuilder().setKey(bs(key)).setValue(bs(value)).build();
    }

    protected static String tblKey(GeneratedDatabaseTables.Column tableColumn, String primKey)
    {
        return DbEtcd.LINSTOR_PREFIX + tableColumn.getTable().name() + "/" + primKey + "/" + tableColumn.name;
    }
}
