package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.HashMap;

@K8sCrdMigration(
    description = "Migrate to SpaceTrackingV2",
    version = 4
)
public class Migration_04_v1_17_0_SpaceTrackingV2 extends BaseK8sCrdMigration
{
    public Migration_04_v1_17_0_SpaceTrackingV2()
    {
        super(GenCrdV1_17_0.createMigrationContext());
    }

    @SuppressWarnings("unchecked")
    @Override
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        HashMap<String, LinstorCrd<LinstorSpec<?, ?>>> crdMap;
        crdMap = (HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>) txFrom.getCrd(
            GenCrdV1_17_0.GeneratedDatabaseTables.SPACE_HISTORY
        );
        for (LinstorCrd<LinstorSpec<?, ?>> value : crdMap.values())
        {
            txTo.delete(GenCrdV1_17_0.GeneratedDatabaseTables.SPACE_HISTORY, value);
        }

        crdMap.clear();

        crdMap = (HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>) txTo.getCrd(
            GenCrdV1_17_0.GeneratedDatabaseTables.SATELLITES_CAPACITY
        );
        for (LinstorCrd<LinstorSpec<?, ?>> value : crdMap.values())
        {
            txTo.delete(GenCrdV1_17_0.GeneratedDatabaseTables.SATELLITES_CAPACITY, value);
        }

        return null;
    }
}
