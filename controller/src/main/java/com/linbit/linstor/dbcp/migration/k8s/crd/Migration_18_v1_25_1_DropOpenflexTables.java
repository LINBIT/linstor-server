package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_25_1;

@K8sCrdMigration(
    description = "Drop OpenFlex tables",
    version = 18
)
public class Migration_18_v1_25_1_DropOpenflexTables extends BaseK8sCrdMigration
{
    public Migration_18_v1_25_1_DropOpenflexTables()
    {
        super(
            GenCrdV1_19_1.createMigrationContext(),
            GenCrdV1_25_1.createMigrationContext()
        );
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        // The OpenFlex CRDs no longer exist in v1_25_1, so updating the CRD schemas deletes them together with all of
        // their CRs. This migration originally also deleted the CRs through txFrom, which never took effect (txFrom
        // was not committed) and would fail now that it is, since the CRDs are already gone at that point.
        updateCrdSchemaForAllTables();

        return null;
    }
}
