package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;
import java.util.TreeMap;
import java.util.Map;

@EtcdMigration(
    description = "Change SpaceTracking persistence path",
    version = 48
)
public class Migration_21_SpaceTrackingPathFix extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        fixPath(tx, prefix, "TRACKING_DATE/");
        fixPath(tx, prefix, "SATELLITES_CAPACITY/");
        fixPath(tx, prefix, "SPACE_HISTORY/");
    }

    private void fixPath(EtcdTransaction tx, String prefix, String tableName)
    {
        TreeMap<String, String> entriesMap = tx.get(tableName);
        for (Map.Entry<String, String> entry : entriesMap.entrySet())
        {
            tx.put(prefix + entry.getKey(), entry.getValue());
        }
        tx.delete(tableName, true);
    }
}
