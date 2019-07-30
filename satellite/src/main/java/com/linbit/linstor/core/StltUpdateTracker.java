package com.linbit.linstor.core;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;

import reactor.core.publisher.Flux;

import java.util.UUID;

public interface StltUpdateTracker
{
    Flux<ApiCallRc> updateController();
    Flux<ApiCallRc> updateNode(UUID nodeUuid, NodeName name);
    Flux<ApiCallRc> updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName);
    Flux<ApiCallRc> updateStorPool(UUID storPoolUuid, StorPoolName storPoolName);
    Flux<ApiCallRc> updateSnapshot(UUID snapshotUuid, ResourceName resourceName, SnapshotName snapshotName);

    boolean isEmpty();
}
