package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Move all storage pool name properties into a simple single one",
    version = 38
)
public class Migration_11_Disable_PlainSSLConnector extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        tx.put(prefix + "PROPS_CONTAINERS//CTRLCFG:netcom/PlainConnector/bindaddress", "");
        tx.put(prefix + "PROPS_CONTAINERS//CTRLCFG:netcom/SslConnector/bindaddress", "");
    }
}
