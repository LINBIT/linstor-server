package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;

import java.util.Map;
import java.util.UUID;

public interface StltUpdateTracker
{
    void updateNode(UUID nodeUuid, NodeName name);
    void updateResourceDfn(UUID rscDfnUuid, ResourceName name);
    void updateResource(ResourceName rscName, Map<NodeName, UUID> updNodeSet);
    void updateStorPool(UUID storPoolUuid, StorPoolName storPoolName);
    void checkResource(UUID rscUuid, ResourceName name);
    void checkMultipleResources(Map<ResourceName, UUID> rscMap);
}
