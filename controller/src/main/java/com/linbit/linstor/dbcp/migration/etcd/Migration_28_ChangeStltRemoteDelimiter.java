package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Change StltRemote delimiter",
    version = 55
)
public class Migration_28_ChangeStltRemoteDelimiter extends BaseEtcdMigration
{
    private static final String OLD_BASE_KEY = "SEC_ACL_MAP//remote/.STLT:";
    private static final String NEW_BASE_KEY = "SEC_ACL_MAP//remote/.STLT;";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        String base = prefix + OLD_BASE_KEY;
        int baseKeyLen = base.length();
        TreeMap<String, String> props = tx.get(base);

        for (Entry<String, String> entry : props.entrySet())
        {
            String oldFullEtcdKey = entry.getKey();
            String newEtcdKey = prefix + NEW_BASE_KEY + oldFullEtcdKey.substring(baseKeyLen);

            tx.delete(oldFullEtcdKey, false);
            tx.put(newEtcdKey, entry.getValue());
        }
    }
}
