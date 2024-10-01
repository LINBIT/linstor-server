package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_18_2;

@K8sCrdMigration(
    description = "Add schedule table",
    version = 6
)
public class Migration_06_v1_18_2_AddScheduleTable extends BaseK8sCrdMigration
{
    public Migration_06_v1_18_2_AddScheduleTable()
    {
        super(
            GenCrdV1_17_0.createMigrationContext(),
            GenCrdV1_18_2.createMigrationContext()
        );
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext ignoredCtxRef) throws Exception
    {
        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        return null;
    }
}
