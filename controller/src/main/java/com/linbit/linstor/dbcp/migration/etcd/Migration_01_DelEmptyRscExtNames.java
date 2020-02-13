package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Delete empty external name entries",
    version = 1
)
// corresponds to SQL Migration_2019_09_09_ExtNameFix
public class Migration_01_DelEmptyRscExtNames extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx)
    {
        TreeMap<String, String> rscDfnTbl = tx.get(LINSTOR_PREFIX_PRE_34 + "RESOURCE_DEFINITIONS", true);

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
