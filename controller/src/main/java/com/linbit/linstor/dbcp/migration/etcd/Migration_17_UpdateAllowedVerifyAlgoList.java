package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Update allowed verify algorithm list and fix incorrect controller disable",
    version = 44
)
public class Migration_17_UpdateAllowedVerifyAlgoList extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        tx.put(prefix + "PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-verify-algo-allowed-list",
            "crct10dif-pclmul;crct10dif-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic");
        final String stltKey = prefix + "PROPS_CONTAINERS/STLTCFG:DrbdOptions/auto-verify-algo-disable";
        final @Nullable String disabled = tx.getFirstValue(stltKey);
        if (disabled != null)
        {
            tx.put(prefix + "PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-verify-algo-disable", disabled);
            tx.delete(stltKey, false);
        }
    }
}
