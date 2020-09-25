package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@EtcdMigration(
    description = "Move all storage pool name properties into a simple single one",
    version = 37
)
public class Migration_10_ConsolidatePoolProperty extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        TreeMap<String, String> allRscGrps = tx.get(prefix + "PROPS_CONTAINERS//storPoolConf", true);

        HashMap<String, String> newKeys = new HashMap<>();
        for (Map.Entry<String, String> entry : allRscGrps.entrySet())
        {
            if (entry.getKey().endsWith("StorDriver/LvmVg") ||
                entry.getKey().endsWith("StorDriver/ZPool") ||
                entry.getKey().endsWith("StorDriver/ZPoolThin") ||
                entry.getKey().endsWith("StorDriver/Openflex/StorPool") ||
                entry.getKey().endsWith("StorDriver/FileDir"))
            {
                int iStorDriver = entry.getKey().lastIndexOf("StorDriver/");
                if (iStorDriver > 0)
                {
                    newKeys.put(
                        entry.getKey().substring(0, iStorDriver) + "StorDriver/" + ApiConsts.KEY_STOR_POOL_NAME,
                        entry.getValue());
                    tx.delete(entry.getKey(), false);
                }
            }
        }

        for (Map.Entry<String, String> entry : allRscGrps.entrySet())
        {
            if (entry.getKey().endsWith("StorDriver/ThinPool"))
            {
                int iStorDriver = entry.getKey().lastIndexOf("StorDriver/");
                if (iStorDriver > 0)
                {
                    final String newKey = entry.getKey().substring(0, iStorDriver) +
                        "StorDriver/" + ApiConsts.KEY_STOR_POOL_NAME;
                    newKeys.put(newKey, newKeys.get(newKey) + "/" + entry.getValue());
                    tx.delete(entry.getKey(), false);
                }
            }
        }

        for (Map.Entry<String, String> entry : newKeys.entrySet())
        {
            tx.put(entry.getKey(), entry.getValue());
        }
    }
}
