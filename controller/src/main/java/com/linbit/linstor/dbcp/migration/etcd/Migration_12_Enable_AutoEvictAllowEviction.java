package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Enable AutoEvictAllowEviction",
    version = 39
)
public class Migration_12_Enable_AutoEvictAllowEviction extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        tx.put(prefix + "PROPS_CONTAINERS//CTRLCFG:DrbdOptions/AutoEvictAllowEviction", "True");
    }
}
