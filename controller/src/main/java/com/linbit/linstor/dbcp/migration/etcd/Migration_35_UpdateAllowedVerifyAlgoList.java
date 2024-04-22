package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Update verify algorithm list",
    version = 62
)
public class Migration_35_UpdateAllowedVerifyAlgoList extends BaseEtcdMigration
{
    private static final String KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST = "PROPS_CONTAINERS//CTRLCFG:" +
        ApiConsts.NAMESPC_DRBD_OPTIONS + "/auto-verify-algo-allowed-list/PROP_VALUE";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        tx.put(
            prefix + KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST,
            "crct10dif;crc32c;sha384;sha512;sha256;sha1;md5;windrbd"
        );
    }
}
