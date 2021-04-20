package com.linbit.linstor.core;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;

import java.util.UUID;

import reactor.core.publisher.Flux;

public interface StltUpdateTracker
{
    Flux<ApiCallRc> updateController();
    Flux<ApiCallRc> updateNode(UUID nodeUuid, NodeName name);
    Flux<ApiCallRc> updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName);
    Flux<ApiCallRc> updateStorPool(UUID storPoolUuid, StorPoolName storPoolName);
    Flux<ApiCallRc> updateSnapshot(UUID snapshotUuid, ResourceName resourceName, SnapshotName snapshotName);
    Flux<ApiCallRc> updateExternalFile(UUID externalFileUuidRef, ExternalFileName externalFileNameRef);

    Flux<ApiCallRc> updateS3Remote(UUID remoteUuid, RemoteName remoteName);

    boolean isEmpty();
}
