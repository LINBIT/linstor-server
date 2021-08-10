package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.UUID;

@EtcdMigration(
    description = "Generate ID for local linstor cluster",
    version = 45
)
public class Migration_18_GenerateClusterId extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        String clusterId = UUID.randomUUID().toString();
        tx.put(
            prefix + "PROPS_CONTAINERS//CTRLCFG:Cluster/LocalID",
            clusterId
        );
        tx.put(
            prefix + "PROPS_CONTAINERS/STLTCFG:Cluster/LocalID",
            clusterId
        );
    }
}
