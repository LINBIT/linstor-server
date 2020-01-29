package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

// corresponds to SQL Migration_2019_09_09_ExtNameFix
public class Migration_01_DelEmptyRscExtNames extends EtcdMigration
{
    public static void migrate(EtcdTransaction tx)
    {
        TreeMap<String, String> rscDfnTbl = tx.get(EtcdUtils.buildKey("RESOURCE_DEFINITIONS"), true);

        for (Entry<String, String> entry : rscDfnTbl.entrySet())
        {
            if (entry.getKey().endsWith("RESOURCE_EXTERNAL_NAME") &&
                entry.getValue().trim().isEmpty())
            {
                tx.delete(entry.getKey(), false);
            }
        }
    }
}
