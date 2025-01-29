package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes;
import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes.Changes;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@EtcdMigration(
    description = "Split snapshotted resource properties - Fix more properties",
    version = 65
)
public class Migration_38_SplitSnapPropsFromRscProps_FixRscGrpInstanceName extends BaseEtcdMigration
{
    private static final String ETCD_CLM_NAME_PROP_VALUE = "/PROP_VALUE";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        final String prefixedDbTableStr = prefix + "PROPS_CONTAINERS/";
        final int prefixedTblKeyLen = prefixedDbTableStr.length();

        Map<String, String> allPropEtcdEntries = tx.get(prefixedDbTableStr, true);

        Map<String, Map<String, String>> allProps = new HashMap<>();

        for (Entry<String, String> entry : allPropEtcdEntries.entrySet())
        {
            String etcdKey = entry.getKey();
            String propValue = entry.getValue();

            String combinedPkAndColumn = etcdKey.substring(prefixedTblKeyLen);
            String combinedPk = combinedPkAndColumn.substring(0, combinedPkAndColumn.lastIndexOf("/"));

            String[] pks = combinedPk.split(":");
            String oldInstaneName = pks[0];
            String propKey = pks[1];

            allProps.computeIfAbsent(oldInstaneName, ignored -> new HashMap<>())
                .put(propKey, propValue);
        }

        Changes changes = Migration_2025_01_29_SplitSnapPropsMoreFixes.calculateChanges(allProps);

        for (Entry<String, Set<String>> entry : changes.entriesToOverrideInstanceName.entrySet())
        {
            String oldInstanceName = entry.getKey();

            String oldEtcdKeyPrefix = prefixedDbTableStr + oldInstanceName + ":";
            String newEtcdKeyPrefix = prefixedDbTableStr +
                Migration_2025_01_29_SplitSnapPropsMoreFixes.getNewInstanceName(oldInstanceName) + ":";

            for (String propKey : entry.getValue())
            {
                String oldEtcdKey = oldEtcdKeyPrefix + propKey + ETCD_CLM_NAME_PROP_VALUE;
                tx.delete(oldEtcdKey);

                String newEtcdKey = newEtcdKeyPrefix + propKey + ETCD_CLM_NAME_PROP_VALUE;
                tx.put(newEtcdKey, allProps.get(oldInstanceName).get(propKey));
            }
        }

        for (Entry<String, Map<String, String>> entry : changes.entriesToUpdateValues.entrySet())
        {
            String oldInstanceName = entry.getKey();

            String oldEtcdKeyPrefix = prefixedDbTableStr + oldInstanceName + ":";
            String newEtcdKeyPrefix = prefixedDbTableStr +
                Migration_2025_01_29_SplitSnapPropsMoreFixes.getNewInstanceName(oldInstanceName) + ":";

            for (Entry<String, String> propEntry : entry.getValue().entrySet())
            {
                String propKey = propEntry.getKey();

                String oldEtcdKey = oldEtcdKeyPrefix + propKey + ETCD_CLM_NAME_PROP_VALUE;
                tx.delete(oldEtcdKey);

                String newEtcdKey = newEtcdKeyPrefix + propKey + ETCD_CLM_NAME_PROP_VALUE;
                tx.put(newEtcdKey, propEntry.getValue());
            }
        }
    }
}
