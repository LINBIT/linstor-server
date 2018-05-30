package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolName;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface StltUpdateTracker
{
    void updateController(UUID nodeUuid, NodeName name);
    void updateNode(UUID nodeUuid, NodeName name);
    void updateResourceDfn(UUID rscDfnUuid, ResourceName name);
    void updateResource(ResourceName rscName, Map<NodeName, UUID> updNodeSet);
    void updateStorPool(UUID storPoolUuid, StorPoolName storPoolName);
    void updateSnapshot(ResourceName resourceName, UUID snapshotUuid, SnapshotName snapshotName);
    void checkResource(ResourceName name);
    void checkMultipleResources(Set<ResourceName> rscSet);
}
