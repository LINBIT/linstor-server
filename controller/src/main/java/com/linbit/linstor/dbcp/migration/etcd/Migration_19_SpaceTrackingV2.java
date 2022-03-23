package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Migrate to SpaceTracking V2",
    version = 46
)
public class Migration_19_SpaceTrackingV2 extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        tx.delete(prefix + "SPACE_HISTORY/", true);
        tx.delete(prefix + "SATELLITS_CAPACITY", true);
    }
}
