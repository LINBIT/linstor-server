package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_33_1;

@K8sCrdMigration(
    description = "Add auth tokens table",
    version = 32
)
public class Migration_32_v1_33_1_AddAuthTokensTable extends BaseK8sCrdMigration
{
    public Migration_32_v1_33_1_AddAuthTokensTable()
    {
        super(
            GenCrdV1_31_1.createMigrationContext(),
            GenCrdV1_33_1.createMigrationContext()
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
