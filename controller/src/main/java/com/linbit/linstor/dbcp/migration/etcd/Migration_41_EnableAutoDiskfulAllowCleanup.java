package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map;

@EtcdMigration(
    description = "Enable DrbdOptions/auto-diskful-allow-cleanup on ctrl",
    version = 68
)
public class Migration_41_EnableAutoDiskfulAllowCleanup extends BaseEtcdMigration
{
    private static final String KEY_AUTO_DISKFUL_ALLOW_CLEANUP = "PROPS_CONTAINERS//CTRL:DrbdOptions/auto-diskful-allow-cleanup/PROP_VALUE";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        Map<String, String> result = tx.get(prefix + KEY_AUTO_DISKFUL_ALLOW_CLEANUP);
        if (result.isEmpty())
        {
            tx.put(prefix + KEY_AUTO_DISKFUL_ALLOW_CLEANUP, "True");
        }
    }
}
