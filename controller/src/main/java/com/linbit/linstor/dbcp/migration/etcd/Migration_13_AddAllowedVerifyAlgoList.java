package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Add allowed verify algorithm list",
    version = 40
)
public class Migration_13_AddAllowedVerifyAlgoList extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        tx.put(prefix + "PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-verify-algo-allowed-list",
            "crct10dif-pclmul;crc32-pclmul;crc32c-intel;" +
            "crct10dif-generic;crc32c-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic");
    }
}
