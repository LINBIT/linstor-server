package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0;

@K8sCrdMigration(
    description = "fix rollback specifications",
    version = 2
)
public class Migration_02_v1_15_0_fixRollbackSpec extends BaseK8sCrdMigration
{
    public Migration_02_v1_15_0_fixRollbackSpec()
    {
        super(GenCrdV1_15_0.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext ignoredCtxRef) throws Exception
    {
        // load data from database that needs to change

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        // write modified data to database

        // nothing to write, just update rollback-schema

        return null;
    }
}
