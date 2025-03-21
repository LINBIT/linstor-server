package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbcp.migration.Migration_2024_11_21_CreateSnapshotsFromZfsClones;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@EtcdMigration(
    description = "Add property for satellites to mark CF_ snapshots for deletion",
    version = 67
)
public class Migration_40_CreateSnapshotsFromZfsClones extends BaseEtcdMigration
{
    private static final String ETCD_CLM_NAME_PROP_VALUE = "/PROP_VALUE";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        final String prefixedDbTableStr = prefix + "NODE_STOR_POOL/";
        final int prefixedTblKeyLen = prefixedDbTableStr.length();
        TreeMap<String, String> allNodeStorPool = tx.get(prefix + "NODE_STOR_POOL", true);

        Set<String> alreadySet = new HashSet<>();
        for (Map.Entry<String, String> entry : allNodeStorPool.entrySet())
        {
            String value = entry.getValue();
            if (entry.getKey().endsWith("DRIVER_NAME") && (value.equalsIgnoreCase(
                DeviceProviderKind.ZFS_THIN.name()) || value.equalsIgnoreCase(DeviceProviderKind.ZFS.name())))
            {
                String combinedPkAndColumn = entry.getKey().substring(prefixedTblKeyLen);
                String combinedPk = combinedPkAndColumn.substring(0, combinedPkAndColumn.lastIndexOf("/"));

                String[] pks = combinedPk.split(":");
                String nodeName = pks[0];

                if (!alreadySet.contains(nodeName))
                {
                    String propInstance = String.format("/NODES/%s", nodeName);
                    String propsPrefix = prefix + "PROPS_CONTAINERS/" + propInstance + ":";
                    String key = propsPrefix + Migration_2024_11_21_CreateSnapshotsFromZfsClones.PROP_KEY_STLT_MIGRATION +
                        ETCD_CLM_NAME_PROP_VALUE;
                    tx.put(key, Migration_2024_11_21_CreateSnapshotsFromZfsClones.PROP_VALUE_STLT_MIGRATION);
                    alreadySet.add(nodeName);
                }
            }
        }
    }
}
