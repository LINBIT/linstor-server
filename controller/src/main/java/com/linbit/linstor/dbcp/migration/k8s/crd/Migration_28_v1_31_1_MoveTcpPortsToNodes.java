package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1.LayerDrbdResources;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1.LayerDrbdResourcesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1.LayerResourceIdsSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Collection;
import java.util.HashMap;

@K8sCrdMigration(
    description = "Move TCP Port number allocation from RscDfn to Rsc",
    version = 28
)
public class Migration_28_v1_31_1_MoveTcpPortsToNodes extends BaseK8sCrdMigration
{
    public Migration_28_v1_31_1_MoveTcpPortsToNodes()
    {
        super(
            GenCrdV1_27_1.createMigrationContext(),
            GenCrdV1_31_1.createMigrationContext()
        );
    }

    @Override
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        Collection<GenCrdV1_27_1.LayerDrbdResourceDefinitionsSpec> oldLayerDrbdRscDfnCollection =
            txFrom.<GenCrdV1_27_1.LayerDrbdResourceDefinitions, GenCrdV1_27_1.LayerDrbdResourceDefinitionsSpec>getSpec(
                GenCrdV1_27_1.GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS
            ).values();

        HashMap<String, GenCrdV1_27_1.ResourceConnectionsSpec> oldRscConMap =
            txFrom.<GenCrdV1_27_1.ResourceConnections, GenCrdV1_27_1.ResourceConnectionsSpec>getSpec(
                GenCrdV1_27_1.GeneratedDatabaseTables.RESOURCE_CONNECTIONS
            );

        updateCrdSchemaForAllTables();

        updateDrbdRscTcpPortList(txTo, oldLayerDrbdRscDfnCollection);
        updateRscConnDrbdProxyPorts(txTo, oldRscConMap);

        return null;
    }

    private void updateDrbdRscTcpPortList(
        K8sCrdTransaction txTo,
        Collection<GenCrdV1_27_1.LayerDrbdResourceDefinitionsSpec> oldLayerDrbdRscDfnCollection
    )
        throws DatabaseException
    {
        HashMap<String, GenCrdV1_27_1.LayerDrbdResourceDefinitionsSpec> oldLayerDrbdRscDfnDataByRscName = new HashMap<>();
        for (GenCrdV1_27_1.LayerDrbdResourceDefinitionsSpec oldLayerDrbdRscDfn : oldLayerDrbdRscDfnCollection)
        {
            if (oldLayerDrbdRscDfn.snapshotName.isEmpty() && oldLayerDrbdRscDfn.resourceNameSuffix.isEmpty())
            {
                oldLayerDrbdRscDfnDataByRscName.put(oldLayerDrbdRscDfn.resourceName, oldLayerDrbdRscDfn);
            }
        }

        Collection<GenCrdV1_31_1.LayerResourceIdsSpec> newLayerRscIdCollection =
            txTo.<GenCrdV1_31_1.LayerResourceIds, GenCrdV1_31_1.LayerResourceIdsSpec>getSpec(
                GenCrdV1_31_1.GeneratedDatabaseTables.LAYER_RESOURCE_IDS
            ).values();

        HashMap<Integer, GenCrdV1_31_1.LayerResourceIdsSpec> newLriByIdMap = new HashMap<>();
        for (LayerResourceIdsSpec newLriSpec : newLayerRscIdCollection)
        {
            newLriByIdMap.put(newLriSpec.layerResourceId, newLriSpec);
        }

        Collection<GenCrdV1_31_1.LayerDrbdResourcesSpec> newLayerDrbdRscCollection =
            txTo.<GenCrdV1_31_1.LayerDrbdResources, GenCrdV1_31_1.LayerDrbdResourcesSpec>getSpec(
                GenCrdV1_31_1.GeneratedDatabaseTables.LAYER_DRBD_RESOURCES
            ).values();

        for (LayerDrbdResourcesSpec newDrbdRscSpec : newLayerDrbdRscCollection)
        {
            @Nullable LayerResourceIdsSpec newLriSpec = newLriByIdMap.get(newDrbdRscSpec.layerResourceId);
            if (newLriSpec == null)
            {
                throw new DatabaseException(
                    "LayerResourceId " + newDrbdRscSpec.layerResourceId + " not found!"
                );
            }
            String tcpPortListStr;
            if (newLriSpec.snapshotName.isEmpty())
            {
                tcpPortListStr = "[" + oldLayerDrbdRscDfnDataByRscName.get(newLriSpec.resourceName).tcpPort + "]";
            }
            else
            {
                tcpPortListStr = "[-1]";
            }
            LayerDrbdResources updatedDrbdRscSpec = GenCrdV1_31_1.createLayerDrbdResources(
                newDrbdRscSpec.layerResourceId,
                newDrbdRscSpec.peerSlots,
                newDrbdRscSpec.alStripes,
                newDrbdRscSpec.alStripeSize,
                newDrbdRscSpec.flags,
                newDrbdRscSpec.nodeId,
                tcpPortListStr
            );

            txTo.upsert(GenCrdV1_31_1.GeneratedDatabaseTables.LAYER_DRBD_RESOURCES, updatedDrbdRscSpec);
        }
    }

    private void updateRscConnDrbdProxyPorts(
        K8sCrdTransaction txToRef,
        HashMap<String, GenCrdV1_27_1.ResourceConnectionsSpec> oldRscConMapRef
    )
    {
        for (GenCrdV1_27_1.ResourceConnectionsSpec oldRscConSpec : oldRscConMapRef.values())
        {
            txToRef.upsert(
                GenCrdV1_31_1.GeneratedDatabaseTables.RESOURCE_CONNECTIONS,
                GenCrdV1_31_1.createResourceConnections(
                    oldRscConSpec.uuid,
                    oldRscConSpec.nodeNameSrc,
                    oldRscConSpec.nodeNameDst,
                    oldRscConSpec.resourceName,
                    oldRscConSpec.snapshotName,
                    oldRscConSpec.flags,
                    oldRscConSpec.tcpPort,
                    oldRscConSpec.tcpPort
                )
            );
        }
    }
}
