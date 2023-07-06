package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Change PropsValue key format",
    version = 57
)
public class Migration_30_ChangePropsValueKeyFormat extends BaseEtcdMigration
{
    /*
     * with the database export and import feature, we must not rely on special driver-specific key modifications.
     * That means we need to migrate all
     * "/LINSTOR/PROPS_CONTAINERS//CTRLCFG:defaultDebugSslConnector"
     * to
     * "/LINSTOR/PROPS_CONTAINERS//CTRLCFG:defaultDebugSslConnector/PROP_VALUE"
     * so that the props-table behaves the same way as other tables, even if it has only a single non-primary column.
     */

    private static final String TBL = "PROPS_CONTAINERS/";
    private static final String COL_NAME_TO_APPEND = "/PROP_VALUE";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        String base = prefix + TBL;
        TreeMap<String, String> props = tx.get(base);

        for (Entry<String, String> entry : props.entrySet())
        {
            String oldFullEtcdKey = entry.getKey();
            String newEtcdKey = oldFullEtcdKey + COL_NAME_TO_APPEND;

            tx.delete(oldFullEtcdKey, false);
            tx.put(newEtcdKey, entry.getValue());
        }
    }
}
