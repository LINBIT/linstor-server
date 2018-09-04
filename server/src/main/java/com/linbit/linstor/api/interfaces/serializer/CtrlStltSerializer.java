package com.linbit.linstor.api.interfaces.serializer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.SnapshotState;

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
        CtrlStltSerializerBuilder primaryRequest(String rscName, String rscUuid);

        CtrlStltSerializerBuilder authMessage(UUID nodeUuid, String nodeName, byte[] sharedSecret);

        CtrlStltSerializerBuilder changedController(UUID nodeUuid, String nodeName);
        CtrlStltSerializerBuilder changedNode(UUID nodeUuid, String nodeName);
        CtrlStltSerializerBuilder changedResource(UUID rscUuid, String rscName);
        CtrlStltSerializerBuilder changedStorPool(UUID storPoolUuid, String storPoolName);
        CtrlStltSerializerBuilder changedSnapshot(String rscName, UUID snapshotUuid, String snapshotName);

        CtrlStltSerializerBuilder controllerData(long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder nodeData(
            Node node,
            Collection<Node> relatedNodes,
            long fullSyncTimestamp,
            long updateId
        );
        CtrlStltSerializerBuilder deletedNodeData(String nodeNameStr, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder resourceData(Resource localResource, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder deletedResourceData(String rscNameStr, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder storPoolData(StorPool storPool, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder deletedStorPoolData(String storPoolName, long fullSyncTimestamp, long updateId);
        CtrlStltSerializerBuilder snapshotData(Snapshot snapshot, long fullSyncId, long updateId);
        CtrlStltSerializerBuilder endedSnapshotData(
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
        CtrlStltSerializerBuilder notifyResourceDeleted(
            String nodeName,
            String resourceName,
            UUID rscUuid,
            Map<StorPool, SpaceInfo> freeSpaceMap
        );
        CtrlStltSerializerBuilder notifyResourceApplied(
            Resource resource,
            Map<StorPool, SpaceInfo> freeSpaceMap
        );
        CtrlStltSerializerBuilder notifyVolumeResized(
            String nodeName,
            String resourceName,
            int volumeNr,
            long vlmSize
        );
        CtrlStltSerializerBuilder notifyDrbdVolumeResized(
            String nodeName,
            String resourceName,
            int volumeNr
        );
        CtrlStltSerializerBuilder notifyVolumeDeleted(
            String nodeName,
            String resourceName,
            int volumeNr,
            long freeSpace,
            UUID vlmUuid
        );

        CtrlStltSerializerBuilder requestControllerUpdate();
        CtrlStltSerializerBuilder requestNodeUpdate(UUID nodeUuid, String nodeName);
        CtrlStltSerializerBuilder requestResourceDfnUpdate(UUID rscDfnUuid, String rscName);
        CtrlStltSerializerBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName);
        CtrlStltSerializerBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName);
        CtrlStltSerializerBuilder requestSnapshotUpdate(
            String rscName,
            UUID snapshotUuid,
            String snapshotName
        );

        CtrlStltSerializerBuilder cryptKey(byte[] masterKey, long timestamp, long updateId);

        CtrlStltSerializerBuilder inProgressSnapshotEvent(SnapshotState snapshotState);
    }
}
