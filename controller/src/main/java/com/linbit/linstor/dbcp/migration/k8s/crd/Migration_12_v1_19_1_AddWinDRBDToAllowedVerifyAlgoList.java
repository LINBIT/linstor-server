package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;

@K8sCrdMigration(
    description = "Add WinDRBD to allowed verify algorithm list",
    version = 12
)
public class Migration_12_v1_19_1_AddWinDRBDToAllowedVerifyAlgoList extends BaseK8sCrdMigration
{
    private static final String PROPS_INSTANCE = "/CTRLCFG";
    private static final String KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST = ApiConsts.NAMESPC_DRBD_OPTIONS +
        "/auto-verify-algo-allowed-list";

    public Migration_12_v1_19_1_AddWinDRBDToAllowedVerifyAlgoList()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        txTo.upsert(
            GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
            GenCrdV1_19_1.createPropsContainers(
                PROPS_INSTANCE,
                KEY_AUTO_VERIFY_ALGO_ALLOWED_LIST,
                "crct10dif-pclmul;crct10dif-generic;crc32c-intel;crc32c-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic;windrbd"
            )
        );
        return null;
    }
}
