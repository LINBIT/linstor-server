package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbcp.migration.Migration_2026_02_16_RenameExtCmdWaitTo;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.HashMap;

@K8sCrdMigration(
    description = "Rename ExtCmdWaitTimeout to ExtCmd/WaitTimeout",
    version = 30
)
public class Migration_30_v1_31_1_RenameExtCmdWaitTo extends BaseK8sCrdMigration
{
    public Migration_30_v1_31_1_RenameExtCmdWaitTo()
    {
        super(
            GenCrdV1_31_1.createMigrationContext()
        );
    }

    @Override
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        HashMap<String, GenCrdV1_31_1.PropsContainers> propCrdMap = txFrom.<GenCrdV1_31_1.PropsContainers, GenCrdV1_31_1.PropsContainersSpec>getCrd(
            GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
            crd -> crd.getSpec().propKey.equals(
                Migration_2026_02_16_RenameExtCmdWaitTo.PROP_KEY_OLD_EXT_CMD_WAIT_TIMEOUT
            )
        );

        for (GenCrdV1_31_1.PropsContainers oldCrd : propCrdMap.values())
        {
            GenCrdV1_31_1.PropsContainersSpec oldSpec = oldCrd.getSpec();
            txTo.create(
                GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
                GenCrdV1_31_1.createPropsContainers(
                    oldSpec.propsInstance,
                    Migration_2026_02_16_RenameExtCmdWaitTo.PROP_KEY_NEW_EXT_CMD_WAIT_TIMEOUT,
                    oldSpec.propValue
                )
            );
            txFrom.delete(GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS, oldCrd);
        }
        return null;
    }
}
