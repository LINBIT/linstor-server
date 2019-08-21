package com.linbit.linstor.core.objects;

import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.client.kv.KvClient;
import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.transaction.TransactionMgrETCD;

import javax.inject.Provider;

import static com.ibm.etcd.client.KeyUtils.bs;

public class EtcdDbDriver
{
    protected final GeneratedDatabaseTables.Table table;
    protected final Provider<TransactionMgrETCD> transMgrProvider;

    public EtcdDbDriver(GeneratedDatabaseTables.Table tableRef, Provider<TransactionMgrETCD> transMgrProviderRef)
    {
        table = tableRef;
        transMgrProvider = transMgrProviderRef;
    }

    protected KvClient getClient()
    {
        return transMgrProvider.get().getClient();
    }

    public String tblKey(String primaryKey, GeneratedDatabaseTables.Column columnName)
    {
        return tblKey(table, primaryKey, columnName.name);
    }

    public static String tblKey(GeneratedDatabaseTables.Table table, String primKey, String columnName)
    {
        return DbEtcd.LINSTOR_PREFIX + table.name() + "/" + primKey + "/" + columnName;
    }

    public static PutRequest putReq(String key, String value)
    {
        return PutRequest.newBuilder().setKey(bs(key)).setValue(bs(value)).build();
    }
}
