package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainersSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Collection;

@K8sCrdMigration(
    description = "Disable AutoResyncAfter",
    version = 11
)
public class Migration_11_v1_19_1_DisableResyncAfter extends BaseK8sCrdMigration
{
    private final static String KEY_AUTO_RESYNC_AFTER = "DrbdOptions/auto-resync-after-disable";

    public Migration_11_v1_19_1_DisableResyncAfter()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        Collection<PropsContainers> propsContainers = txFrom.<PropsContainers, PropsContainersSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
            propsCont -> propsCont.getSpec().propsInstance.startsWith("/CTRLCFG") &&
                propsCont.getSpec().propKey.equals(KEY_AUTO_RESYNC_AFTER)
        ).values();

        if (propsContainers.isEmpty())
        {
            txTo.create(
                GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
                GenCrdV1_19_1.createPropsContainers(
                    "/CTRLCFG",
                    KEY_AUTO_RESYNC_AFTER,
                    "True"
                )
            );
        }
        return null;
    }
}
