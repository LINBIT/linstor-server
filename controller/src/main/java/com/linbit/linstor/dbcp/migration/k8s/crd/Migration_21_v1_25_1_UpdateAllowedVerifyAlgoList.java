package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_25_1;

@K8sCrdMigration(
    description = "Update allowed verify algorithm list",
    version = 21
)
public class Migration_21_v1_25_1_UpdateAllowedVerifyAlgoList extends BaseK8sCrdMigration
{
    private static final String PROPS_INSTANCE = "/CTRLCFG";
    private static final String KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST = ApiConsts.NAMESPC_DRBD_OPTIONS +
        "/auto-verify-algo-allowed-list";

    public Migration_21_v1_25_1_UpdateAllowedVerifyAlgoList()
    {
        super(GenCrdV1_25_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        txTo.upsert(
            GenCrdV1_25_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
            GenCrdV1_25_1.createPropsContainers(
                PROPS_INSTANCE,
                KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST,
                "crct10dif;crc32c;sha384;sha512;sha256;sha1;md5;windrbd"
            )
        );
        return null;
    }
}
