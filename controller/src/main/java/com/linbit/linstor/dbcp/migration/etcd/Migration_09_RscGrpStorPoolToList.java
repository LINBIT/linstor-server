package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map;
import java.util.TreeMap;

@EtcdMigration(
    description = "Old resource group storpool strings should be string arrays",
    version = 36
)
public class Migration_09_RscGrpStorPoolToList extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        TreeMap<String, String> allRscGrps = tx.get(prefix + "RESOURCE_GROUPS", true);

        for (Map.Entry<String, String> entry : allRscGrps.entrySet())
        {
            if (entry.getKey().endsWith("POOL_NAME") && !entry.getValue().startsWith("["))
            {
                final String newVal = entry.getValue().isEmpty() ? "[]" : "[\"" + entry.getValue() + "\"]";
                tx.put(entry.getKey(), newVal);
            }
        }
    }
}
