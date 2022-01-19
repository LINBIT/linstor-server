package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0;

@K8sCrdMigration(
    description = "fix rollback specifications",
    version = 2
)
public class Migration_2_v1_15_0_fixRollbackSpec extends BaseK8sCrdMigration
{
    public Migration_2_v1_15_0_fixRollbackSpec()
    {
        super(
            GenCrdV1_15_0.createTxMgrContext(),
            GenCrdV1_15_0.createTxMgrContext(),
            GenCrdV1_15_0.createSchemaUpdateContext()
        );
    }

    @Override
    public MigrationResult migrateImpl() throws Exception
    {
        // load data from database that needs to change

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        // write modified data to database

        // nothing to write, just update rollback-schema

        return null;
    }
}
