package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map;

@EtcdMigration(
    description = "Disable AutoResyncAfter",
    version = 51
)
public class Migration_24_Disable_AutoResyncAfter extends BaseEtcdMigration
{
    private final static String KEY_AUTO_RESYNC_AFTER =
        "PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-resync-after-disable";
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        Map<String, String> result = tx.get(prefix + KEY_AUTO_RESYNC_AFTER);
        if (result.isEmpty())
        {
            tx.put(prefix + KEY_AUTO_RESYNC_AFTER, "True");
        }
    }
}
