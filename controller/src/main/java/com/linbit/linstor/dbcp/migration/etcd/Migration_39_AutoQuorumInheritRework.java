package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes;
import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes.Changes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.transaction.EtcdTransaction;

import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.PROPS_CONTAINERS;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@EtcdMigration(
    description = "Auto-quorum rework",
    version = 66
)
public class Migration_39_AutoQuorumInheritRework extends BaseEtcdMigration
{
    private static final String ETCD_CLM_NAME_PROP_VALUE = "/PROP_VALUE";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        final String prefixedDbTableStr = prefix + "PROPS_CONTAINERS/";
        final int prefixedTblKeyLen = prefixedDbTableStr.length();

        Map<String, String> allPropEtcdEntries = tx.get(prefixedDbTableStr, true);

        for (Entry<String, String> entry : allPropEtcdEntries.entrySet())
        {
            String combinedPkAndColumn = entry.getKey().substring(prefixedTblKeyLen);
            String combinedPk = combinedPkAndColumn.substring(0, combinedPkAndColumn.lastIndexOf("/"));

            String[] pks = combinedPk.split(":");
            String propInstance = pks[0];
            String propKey = pks[1];

            if (propKey.equalsIgnoreCase("DrbdOptions/auto-quorum"))
            {
                String autoQuorumValue = entry.getValue();
                propInstance = propInstance.equalsIgnoreCase("/CTRL") ? "/STLT" : propInstance;

                String newPrefix = prefixedDbTableStr + propInstance + ":";

                if (autoQuorumValue.equalsIgnoreCase("disabled"))
                {
                    // if disabled set quorum -> off for the props_instance if not already set
                    tx.put(newPrefix + "Internal/Drbd/QuorumSetBy" + ETCD_CLM_NAME_PROP_VALUE, "user");

                    tx.put(newPrefix + "DrbdOptions/Resource/quorum" + ETCD_CLM_NAME_PROP_VALUE, "off");
                }
                else
                {
                    // if not disabled, set the current auto-quorum value to on-no-quorum
                    tx.put(newPrefix + "DrbdOptions/Resource/on-no-quorum" + ETCD_CLM_NAME_PROP_VALUE, autoQuorumValue);
                }

                tx.delete(entry.getKey());
            }
        }
    }
}
