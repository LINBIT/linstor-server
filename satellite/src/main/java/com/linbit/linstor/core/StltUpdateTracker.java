package com.linbit.linstor.core;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.StltUpdateTrackerImpl.AtomicUpdateHolder;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;

import java.util.List;
import java.util.UUID;

import reactor.core.publisher.Flux;

public interface StltUpdateTracker
{
    List<Flux<ApiCallRc>> updateData(AtomicUpdateHolder holderRef);

    Flux<ApiCallRc> updateController();
    Flux<ApiCallRc> updateNode(UUID nodeUuid, NodeName name);
    Flux<ApiCallRc> updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName);
    Flux<ApiCallRc> updateStorPool(UUID storPoolUuid, NodeName nodeNameRef, StorPoolName storPoolName);
    Flux<ApiCallRc> updateSnapshot(UUID snapshotUuid, ResourceName resourceName, SnapshotName snapshotName);
    Flux<ApiCallRc> updateExternalFile(UUID externalFileUuidRef, ExternalFileName externalFileNameRef);

    Flux<ApiCallRc> updateS3Remote(UUID remoteUuid, RemoteName remoteName);

    boolean isEmpty();
}
