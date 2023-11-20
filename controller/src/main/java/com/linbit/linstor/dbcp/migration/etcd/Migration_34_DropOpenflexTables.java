package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Drop openflex tables",
    version = 61
)
public class Migration_34_DropOpenflexTables extends BaseEtcdMigration
{
    private static final String TBL_OF_RD = "LAYER_OPENFLEX_RESOURCE_DEFINITIONS/";
    private static final String TBL_OF_V = "LAYER_OPENFLEX_VOLUMES/";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        tx.delete(prefix + TBL_OF_RD, true);
        tx.delete(prefix + TBL_OF_V, true);
    }
}
