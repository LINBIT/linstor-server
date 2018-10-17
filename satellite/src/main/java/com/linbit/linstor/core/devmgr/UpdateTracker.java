package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolName;
import java.util.Set;
import java.util.UUID;

public interface UpdateTracker
{
    // these methods are called when the controller has sent "Changed" message
    void updateController();
    void updateNode(UUID nodeUuid, NodeName name);
    void updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName);
    void updateStorPool(UUID storPoolUuid, StorPoolName storPoolName);
    void updateSnapshot(UUID snapshotUuid, ResourceName resourceName, SnapshotName snapshotName);

    // these methods are called when the controller has sent "*Data" messages (the actual update-data)
    void controllerUpdateApplied(Set<ResourceName> rscSet);
    void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet);
    void storPoolUpdateApplied(Set<StorPoolName> storPoolSet, Set<ResourceName> rscSet);
    void rscUpdateApplied(Set<Resource.Key> rscSet);
    void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet);
}
