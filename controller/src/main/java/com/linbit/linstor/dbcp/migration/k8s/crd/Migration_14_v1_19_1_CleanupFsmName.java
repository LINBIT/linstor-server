package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.NodeStorPool;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.NodeStorPoolSpec;

import java.util.Collection;

@K8sCrdMigration(
    description = "Cleanup FSM name",
    version = 14
)
public class Migration_14_v1_19_1_CleanupFsmName extends BaseK8sCrdMigration
{
    public Migration_14_v1_19_1_CleanupFsmName()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        // FSM's objProt (with ACLs) were removed in an earlier commit than support for K8s was added.
        // therefore there is nothing to cleanup in secObjProt or in secAcl

        // change SPs FSM name, replacing ":" with ";"
        Collection<NodeStorPool> fsmSPs = txFrom.<NodeStorPool, NodeStorPoolSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.NODE_STOR_POOL,
            crd -> crd.getSpec().freeSpaceMgrName.contains(":")
        ).values();

        for (NodeStorPool sp : fsmSPs)
        {
            NodeStorPoolSpec spec = sp.getSpec();
            txTo.upsert(
                GenCrdV1_19_1.GeneratedDatabaseTables.NODE_STOR_POOL,
                GenCrdV1_19_1.createNodeStorPool(
                    spec.uuid,
                    spec.nodeName,
                    spec.poolName,
                    spec.driverName,
                    spec.freeSpaceMgrName.replace(":", ";"),
                    spec.freeSpaceMgrDspName.replace(":", ";"),
                    spec.externalLocking
                )
            );
        }
        return null;
    }
}
