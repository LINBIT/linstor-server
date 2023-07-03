package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Add WinDRBD to allowed verify algorithm list",
    version = 52
)
public class Migration_25_AddWinDRBDToAllowedVerifyAlgoList extends BaseEtcdMigration
{
    private static final String KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST = "PROPS_CONTAINERS//CTRLCFG:" +
        ApiConsts.NAMESPC_DRBD_OPTIONS + "/auto-verify-algo-allowed-list";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        tx.put(
            prefix + KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST,
            "crct10dif-pclmul;crct10dif-generic;crc32c-intel;crc32c-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic;windrbd"
        );
    }
}
