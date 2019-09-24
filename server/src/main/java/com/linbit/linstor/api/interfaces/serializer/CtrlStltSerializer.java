package com.linbit.linstor.api.interfaces.serializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.storage.LsBlkEntry;

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

    public interface CtrlStltSerializerBuilder extends CommonSerializerBuilder
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

        /*
         * Satellite -> Controller
         */
        CtrlStltSerializerBuilder notifyResourceApplied(
            Resource resource,
            Map<StorPool, SpaceInfo> freeSpaceMap
        );

        CtrlStltSerializerBuilder notifyResourceFailed(
            Resource resource,
            ApiCallRc apiCallRc
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

        CtrlStltSerializerBuilder updateFreeCapacities(Map<StorPool, SpaceInfo> spaceInfoMap);

        CtrlStltSerializerBuilder cryptKey(byte[] masterKey, long timestamp, long updateId);

        CommonSerializerBuilder storPoolApplied(
            StorPool storPoolRef,
            SpaceInfo spaceInfoRef,
            boolean supportsSnapshotsRef
        );

        CtrlStltSerializerBuilder requestPhysicalDevices(boolean filter);
        CtrlStltSerializerBuilder physicalDevices(
            List<LsBlkEntry> entries
        );
    }
}
