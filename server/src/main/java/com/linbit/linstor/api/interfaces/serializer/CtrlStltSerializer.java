package com.linbit.linstor.api.interfaces.serializer;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.RaidLevel;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface CtrlStltSerializer extends CommonSerializer
{
    @Override
    CtrlStltSerializerBuilder headerlessBuilder();

    @Override
    CtrlStltSerializerBuilder onewayBuilder(String apiCall);

    @Override
    CtrlStltSerializerBuilder apiCallBuilder(String apiCall, Long apiCallId);

    @Override
    CtrlStltSerializerBuilder answerBuilder(String msgContent, Long apiCallId);

    @Override
    CtrlStltSerializerBuilder completionBuilder(Long apiCallId);

    interface CtrlStltSerializerBuilder extends CommonSerializerBuilder
    {
        /*
         * Controller -> Satellite
         */
        CtrlStltSerializerBuilder primaryRequest(String rscName, String rscUuid, boolean alreadyInitialized);

        CtrlStltSerializerBuilder authMessage(UUID nodeUuid, String nodeName, byte[] sharedSecret);

        CtrlStltSerializerBuilder changedNode(UUID nodeUuid, String nodeName);
        CtrlStltSerializerBuilder changedResource(UUID rscUuid, String rscName);
        CtrlStltSerializerBuilder changedStorPool(UUID storPoolUuid, String storPoolName);
        CtrlStltSerializerBuilder changedSnapshot(String rscName, UUID snapshotUuid, String snapshotName);

        CtrlStltSerializerBuilder changedConfig(StltConfig stltConfig) throws IOException;

        CtrlStltSerializerBuilder controllerData(long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder node(
            Node node,
            Collection<Node> relatedNodes,
            long fullSyncTimestamp,
            long updateId
        );
        CtrlStltSerializerBuilder deletedNode(String nodeNameStr, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder resource(Resource localResource, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder deletedResource(String rscNameStr, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder storPool(StorPool storPool, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder deletedStorPool(String storPoolName, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder snapshot(Snapshot snapshot, long fullSyncId, long updateId);
        CtrlStltSerializerBuilder endedSnapshot(
            String resourceNameStr,
            String snapshotNameStr,
            long fullSyncId,
            long updateId
        );

        CtrlStltSerializerBuilder fullSync(
            Set<Node> nodeSet,
            Set<StorPool> storPools,
            Set<Resource> resources,
            Set<Snapshot> snapshots, long timestamp,
            long updateId
        );

        CtrlStltSerializerBuilder grantsharedStorPoolLocks(Set<SharedStorPoolName> locksRef);

        /*
         * Satellite -> Controller
         */
        CtrlStltSerializerBuilder notifyNodeApplied(Node node);
        CtrlStltSerializerBuilder notifyResourceApplied(
            Resource resource,
            Map<StorPool, SpaceInfo> freeSpaceMap
        );
        CtrlStltSerializerBuilder notifyResourceFailed(
            Resource resource,
            ApiCallRc apiCallRc
        );

        CtrlStltSerializerBuilder notifySnapshotShipped(
            Snapshot snap,
            boolean successRef,
            Set<Integer> vlmNrsWithBlockedPort
        );

        CtrlStltSerializerBuilder requestControllerUpdate();
        CtrlStltSerializerBuilder requestNodeUpdate(UUID nodeUuid, String nodeName);
        CtrlStltSerializerBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName);
        CtrlStltSerializerBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName);
        CtrlStltSerializerBuilder requestSnapshotUpdate(
            String rscName,
            UUID snapshotUuid,
            String snapshotName
        );
        CtrlStltSerializerBuilder requestSharedStorPoolLocks(Set<SharedStorPoolName> sharedSPLocksRef);

        CtrlStltSerializerBuilder updateFreeCapacities(Map<StorPool, SpaceInfo> spaceInfoMap);

        CtrlStltSerializerBuilder cryptKey(byte[] masterKey, long timestamp, long updateId);

        CommonSerializerBuilder storPoolApplied(
            StorPool storPoolRef,
            SpaceInfo spaceInfoRef,
            boolean supportsSnapshotsRef
        );

        CtrlStltSerializerBuilder stltConfigApplied(boolean success) throws IOException;

        CtrlStltSerializerBuilder requestPhysicalDevices(boolean filter);
        CtrlStltSerializerBuilder physicalDevices(
            List<LsBlkEntry> entries
        );
        CtrlStltSerializerBuilder createDevicePool(
            List<String> devicePaths,
            DeviceProviderKind providerKindRef,
            RaidLevel raidLevel,
            String poolName,
            boolean vdoEnabled,
            long vdoLogicalSizeKib,
            long vdoSlabSizeKib
        );

        CtrlStltSerializerBuilder deleteDevicePool(
            List<String> devicePaths,
            DeviceProviderKind providerKindRef,
            String poolName
        );
    }
}
