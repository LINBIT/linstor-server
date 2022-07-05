package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_18_2;

@K8sCrdMigration(
    description = "Add schedule table",
    version = 6
)
public class Migration_6_v1_18_2_AddScheduleTable extends BaseK8sCrdMigration
{
    public Migration_6_v1_18_2_AddScheduleTable()
    {
        super(
            GenCrdV1_17_0.createTxMgrContext(),
            GenCrdV1_18_2.createTxMgrContext(),
            GenCrdV1_18_2.createSchemaUpdateContext()
        );
    }

    @Override
    public MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        return null;
    }
}
