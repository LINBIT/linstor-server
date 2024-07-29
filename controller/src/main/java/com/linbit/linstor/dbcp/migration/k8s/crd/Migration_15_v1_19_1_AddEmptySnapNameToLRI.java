package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LayerDrbdResourceDefinitions;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LayerDrbdResourceDefinitionsSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LayerDrbdVolumeDefinitions;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LayerDrbdVolumeDefinitionsSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LayerResourceIds;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LayerResourceIdsSpec;

import java.util.Collection;

@K8sCrdMigration(
    description = "Add empty snapshot name to LayerResourceId, DrbdRscDfnData and DrbdVlmDfnData",
    version = 15
)
public class Migration_15_v1_19_1_AddEmptySnapNameToLRI extends BaseK8sCrdMigration
{
    private static final String DFLT_SNAP_NAME_FOR_RSC = "";

    public Migration_15_v1_19_1_AddEmptySnapNameToLRI()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        Collection<LayerResourceIds> crdList = txFrom.<LayerResourceIds, LayerResourceIdsSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_RESOURCE_IDS
        ).values();

        for (LayerResourceIds crd : crdList)
        {
            LayerResourceIdsSpec spec = crd.getSpec();
            txTo.upsert(
                GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_RESOURCE_IDS,
                GenCrdV1_19_1.createLayerResourceIds(
                    spec.layerResourceId,
                    spec.nodeName,
                    spec.resourceName,
                    spec.snapshotName == null ? DFLT_SNAP_NAME_FOR_RSC : spec.snapshotName,
                    spec.layerResourceKind,
                    spec.layerResourceParentId,
                    spec.layerResourceSuffix,
                    spec.layerResourceSuspended
                )
            );
        }

        Collection<LayerDrbdResourceDefinitions> drbdRscDfnList = txFrom
            .<LayerDrbdResourceDefinitions, LayerDrbdResourceDefinitionsSpec>getCrd(
                GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS
            )
            .values();
        for (LayerDrbdResourceDefinitions crd : drbdRscDfnList)
        {
            LayerDrbdResourceDefinitionsSpec spec = crd.getSpec();
            // upsert will not find the old entry since we (might) change the snapshot name.
            txTo.delete(GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS, crd);
            txTo.create(
                GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS,
                GenCrdV1_19_1.createLayerDrbdResourceDefinitions(
                    spec.resourceName,
                    spec.resourceNameSuffix,
                    spec.snapshotName == null ? DFLT_SNAP_NAME_FOR_RSC : spec.snapshotName,
                    spec.peerSlots,
                    spec.alStripes,
                    spec.alStripeSize,
                    // snapshot should not occupy a TCP port
                    spec.snapshotName != null ? null : spec.tcpPort,
                    spec.transportType,
                    spec.secret
                )
            );
        }

        Collection<LayerDrbdVolumeDefinitions> drbdVlmDfnList = txFrom
            .<LayerDrbdVolumeDefinitions, LayerDrbdVolumeDefinitionsSpec>getCrd(
                GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS
            )
            .values();
        for (LayerDrbdVolumeDefinitions crd : drbdVlmDfnList)
        {
            LayerDrbdVolumeDefinitionsSpec spec = crd.getSpec();
            // upsert will not find the old entry since we (might) change the snapshot name.
            txTo.delete(GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS, crd);
            txTo.create(
                GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS,
                GenCrdV1_19_1.createLayerDrbdVolumeDefinitions(
                    spec.resourceName,
                    spec.resourceNameSuffix,
                    spec.snapshotName == null ? DFLT_SNAP_NAME_FOR_RSC : spec.snapshotName,
                    spec.vlmNr,
                    spec.vlmMinorNr
                )
            );
        }
        return null;
    }
}
