package com.linbit.linstor.dbcp.migration.etcd;

import java.util.Map.Entry;

import com.linbit.linstor.transaction.ControllerETCDTransactionMgr;

import java.util.TreeMap;

import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

// corresponds to SQL Migration_2019_09_09_ExtNameFix
public class Migration_01_DelEmptyRscExtNames extends EtcdMigration
{
    public static void migrate(ControllerETCDTransactionMgr txMgr)
    {
        FluentTxnOps<?> tx = txMgr.getTransaction();

        TreeMap<String, String> rscDfnTbl = txMgr.readTable("RESOURCE_DEFINITIONS", true);

        for (Entry<String, String> entry : rscDfnTbl.entrySet())
        {
            if (entry.getKey().endsWith("RESOURCE_EXTERNAL_NAME") &&
                entry.getValue().trim().isEmpty())
            {
                tx.delete(delReq(entry.getKey(), false));
            }
        }
    }
}
