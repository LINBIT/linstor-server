package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps;
import com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.SnapshotKey;
import com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.SnapshotVolumeKey;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainersSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.Resources;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourcesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.Volumes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.VolumesSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@K8sCrdMigration(
    description = "Cleanup orphaned Snapshot and SnapshotVolume Properties",
    version = 8
)
public class Migration_08_v1_19_1_CleanupOrphanedSnapAndSnapVlmProps extends BaseK8sCrdMigration
{
    public Migration_08_v1_19_1_CleanupOrphanedSnapAndSnapVlmProps()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        Collection<ResourcesSpec> absResources = txFrom.<Resources, ResourcesSpec>getSpec(
            GenCrdV1_19_1.GeneratedDatabaseTables.RESOURCES
        ).values();
        Collection<VolumesSpec> absVolumes = txFrom.<Volumes, VolumesSpec>getSpec(
            GenCrdV1_19_1.GeneratedDatabaseTables.VOLUMES
        ).values();
        Collection<PropsContainersSpec> propsSpecs = txFrom.<PropsContainers, PropsContainersSpec>getSpec(
            GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS
        ).values();

        Map<String, List<PropsContainersSpec>> propsInstances = new HashMap<>();
        for (PropsContainersSpec propsSpec : propsSpecs)
        {
            String propsInst = propsSpec.propsInstance;
            if (propsInst.startsWith(
                Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.PATH_SNAPSHOTS
            ))
            {
                propsInstances.computeIfAbsent(propsSpec.propsInstance, ignored -> new ArrayList<>())
                    .add(propsSpec);
            }
        }

        Set<SnapshotKey> snapKeySet = new HashSet<>();
        for (ResourcesSpec absRscSpec : absResources)
        {
            if (absRscSpec.snapshotName != null && !absRscSpec.snapshotName.isEmpty())
            {
                snapKeySet.add(new SnapshotKey(absRscSpec.nodeName, absRscSpec.resourceName, absRscSpec.snapshotName));
            }
        }

        Set<SnapshotVolumeKey> snapVlmKeySet = new HashSet<>();
        for (VolumesSpec absVlmSpec : absVolumes)
        {
            if (absVlmSpec.snapshotName != null && !absVlmSpec.snapshotName.isEmpty())
            {
                snapVlmKeySet.add(
                    new SnapshotVolumeKey(
                        absVlmSpec.nodeName,
                        absVlmSpec.resourceName,
                        absVlmSpec.snapshotName,
                        absVlmSpec.vlmNr
                    )
                );
            }
        }

        Collection<String> propsInstancesToDelete = Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps
            .getPropsInstancesToDelete(
                propsInstances.keySet(),
                snapKeySet,
                snapVlmKeySet
            );

        for (String propsInstanceToDelete : propsInstancesToDelete)
        {
            for (PropsContainersSpec propsContainersSpec : propsInstances.get(propsInstanceToDelete))
            {
                txTo.delete(
                    GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
                    GenCrdV1_19_1.specToCrd(propsContainersSpec)
                );
            }
        }

        return null;
    }
}
