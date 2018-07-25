package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolName;

import java.util.Set;
import java.util.UUID;

public interface StltUpdateTracker
{
    void updateController(UUID nodeUuid, NodeName name);
    void updateNode(UUID nodeUuid, NodeName name);
    void updateResourceDfn(UUID rscDfnUuid, ResourceName name);
    void updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName);
    void updateStorPool(UUID storPoolUuid, StorPoolName storPoolName);
    void updateSnapshot(UUID snapshotUuid, ResourceName resourceName, SnapshotName snapshotName);
    void markResourceForDispatch(ResourceName name);
    void markMultipleResourcesForDispatch(Set<ResourceName> rscSet);
}
