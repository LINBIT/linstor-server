package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRc;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface StltUpdateTracker
{
    Flux<ApiCallRc> updateController();
    Flux<ApiCallRc> updateNode(UUID nodeUuid, NodeName name);
    Flux<ApiCallRc> updateResourceDfn(UUID rscDfnUuid, ResourceName name);
    Flux<ApiCallRc> updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName);
    Flux<ApiCallRc> updateStorPool(UUID storPoolUuid, StorPoolName storPoolName);
    Flux<ApiCallRc> updateSnapshot(UUID snapshotUuid, ResourceName resourceName, SnapshotName snapshotName);
}
