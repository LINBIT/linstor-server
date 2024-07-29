package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_25_1;

@K8sCrdMigration(
    description = "Add PeerSlots to RscGrp",
    version = 19
)
public class Migration_19_v1_25_1_AddPeerSlotsToRscGrp extends BaseK8sCrdMigration
{
    public Migration_19_v1_25_1_AddPeerSlotsToRscGrp()
    {
        super(GenCrdV1_25_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        // actually noop, since Migration 18 already applied the structural changes

        return null;
    }
}
