package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_25_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;

@K8sCrdMigration(
    description = "Add X_REPLICAS_ON_DIFFERENT column to RESOURCE_GROUPS",
    version = 22
)
public class Migration_22_v1_27_1_AddXReplicasOnDifferent extends BaseK8sCrdMigration
{
    public Migration_22_v1_27_1_AddXReplicasOnDifferent()
    {
        super(GenCrdV1_25_1.createMigrationContext(), GenCrdV1_27_1.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        updateCrdSchemaForAllTables();
        return null;
    }
}
