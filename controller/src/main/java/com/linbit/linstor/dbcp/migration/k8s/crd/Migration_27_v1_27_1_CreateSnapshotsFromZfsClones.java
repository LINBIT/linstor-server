package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbcp.migration.Migration_2024_11_21_CreateSnapshotsFromZfsClones;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.NodeStorPool;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.NodeStorPoolSpec;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.NODE_STOR_POOL;
import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.PROPS_CONTAINERS;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@K8sCrdMigration(
    description = "Add property for satellites to mark CF_ snapshots for deletion",
    version = 27
)
public class Migration_27_v1_27_1_CreateSnapshotsFromZfsClones extends BaseK8sCrdMigration
{
    public Migration_27_v1_27_1_CreateSnapshotsFromZfsClones()
    {
        super(GenCrdV1_27_1.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;
        Collection<NodeStorPool> crdList = txFrom.<NodeStorPool, NodeStorPoolSpec>getCrd(
            NODE_STOR_POOL
        ).values();

        Set<String> alreadySet = new HashSet<>();
        for (NodeStorPool nodeStorPoolCrd : crdList)
        {
            NodeStorPoolSpec nodeStorPoolSpec = nodeStorPoolCrd.getSpec();
            if (nodeStorPoolSpec.driverName.equalsIgnoreCase(DeviceProviderKind.ZFS_THIN.name()) ||
                nodeStorPoolSpec.driverName.equalsIgnoreCase(DeviceProviderKind.ZFS.name()))
            {
                if (!alreadySet.contains(nodeStorPoolSpec.nodeName))
                {
                    String propInstance = String.format("/NODES/%s", nodeStorPoolSpec.nodeName);
                    txTo.create(PROPS_CONTAINERS,
                        GenCrdV1_27_1.createPropsContainers(
                            propInstance,
                            Migration_2024_11_21_CreateSnapshotsFromZfsClones.PROP_KEY_STLT_MIGRATION,
                            Migration_2024_11_21_CreateSnapshotsFromZfsClones.PROP_VALUE_STLT_MIGRATION
                        )
                    );
                    alreadySet.add(nodeStorPoolSpec.nodeName);
                }
            }
        }

        return null;
    }
}
