package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2026_11_03_CleanupOldCloneSnapshots;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.NodeStorPool;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.NodeStorPoolSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainersSpec;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Set;
import java.util.TreeSet;

/**
 * See {@link Migration_2026_11_03_CleanupOldCloneSnapshots}
 */
@K8sCrdMigration(
    description = "Clean up the leftovers of clones that took their own source snapshots",
    version = 37
)
public class Migration_37_v1_34_0_CleanupOldCloneSnapshots extends BaseK8sCrdMigration
{
    private static final DatabaseTable NODE_STOR_POOL = GenCrdV1_34_0.GeneratedDatabaseTables.NODE_STOR_POOL;
    private static final DatabaseTable PROPS_CONTAINERS = GenCrdV1_34_0.GeneratedDatabaseTables.PROPS_CONTAINERS;

    public Migration_37_v1_34_0_CleanupOldCloneSnapshots()
    {
        super(GenCrdV1_34_0.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        // same CRD version for from and to, so all changes can go through one transaction
        cleanup(migrationCtxRef.txTo);

        return null;
    }

    static void cleanup(K8sCrdTransaction tx)
    {
        Set<String> lvmNodeNames = new TreeSet<>();
        for (NodeStorPool nodeStorPool : tx.<NodeStorPool, NodeStorPoolSpec>getCrd(NODE_STOR_POOL).values())
        {
            NodeStorPoolSpec spec = nodeStorPool.getSpec();
            if (spec.driverName.equalsIgnoreCase(DeviceProviderKind.LVM.name()) ||
                spec.driverName.equalsIgnoreCase(DeviceProviderKind.LVM_THIN.name()))
            {
                lvmNodeNames.add(spec.nodeName);
            }
        }
        for (String nodeName : lvmNodeNames)
        {
            tx.create(
                PROPS_CONTAINERS,
                GenCrdV1_34_0.createPropsContainers(
                    "/NODES/" + nodeName,
                    Migration_2026_11_03_CleanupOldCloneSnapshots.PROP_KEY_STLT_MIGRATION,
                    Migration_2026_11_03_CleanupOldCloneSnapshots.PROP_VALUE_STLT_MIGRATION
                )
            );
        }

        for (PropsContainers oldCloneProp : tx.<PropsContainers, PropsContainersSpec>getCrd(
            PROPS_CONTAINERS,
            crd -> crd.getSpec().propsInstance.startsWith(
                Migration_2026_11_03_CleanupOldCloneSnapshots.PROPS_INSTANCE_PREFIX_RSC
            ) &&
                crd.getSpec().propKey.startsWith(Migration_2026_11_03_CleanupOldCloneSnapshots.PROP_KEY_PREFIX_OLD_CLONE)
        ).values())
        {
            tx.delete(PROPS_CONTAINERS, oldCloneProp);
        }
    }
}
