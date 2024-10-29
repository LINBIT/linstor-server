package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import static com.linbit.linstor.dbcp.migration.Migration_2024_10_24_SplitSnapPropsFromRscPropsAndChangeInstanceNames.getNewInstanceName;

import java.util.Map;
import java.util.Map.Entry;

@EtcdMigration(
    description = "Split snapshotted resource properties from snapshot properties and change instance names",
    version = 63
)
public class Migration_36_SplitSnapPropsFromRscPropsAndChangeInstanceName extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        final String prefixedDbTableStr = prefix + "PROPS_CONTAINERS/";
        final int prefixedTblKeyLen = prefixedDbTableStr.length();

        Map<String, String> allProps = tx.get(prefixedDbTableStr, true);

        for (Entry<String, String> entry : allProps.entrySet())
        {
            String etcdKey = entry.getKey();
            String propValue = entry.getValue();

            String combinedPkAndColumn = etcdKey.substring(prefixedTblKeyLen);
            String combinedPk = combinedPkAndColumn.substring(0, combinedPkAndColumn.lastIndexOf("/"));

            String[] pks = combinedPk.split(":");
            String oldInstaneName = pks[0];
            String propKey = pks[1];

            String newInstanceName = getNewInstanceName(oldInstaneName, propKey);
            if (!oldInstaneName.equals(newInstanceName))
            {
                tx.delete(etcdKey);
                String newEtcdKey = prefixedDbTableStr + newInstanceName + ":" + propKey + "/PROP_VALUE";
                tx.put(newEtcdKey, propValue);
            }
        }
    }
}
