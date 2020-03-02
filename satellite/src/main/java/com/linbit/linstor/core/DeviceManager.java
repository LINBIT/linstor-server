package com.linbit.linstor.core;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer;

import java.util.Set;

public interface DeviceManager extends DrbdStateChange, DeviceLayer.NotificationListener
{
    void controllerUpdateApplied(Set<ResourceName> rscSet);
    void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet);
    void storPoolUpdateApplied(Set<StorPoolName> storPoolSet, Set<ResourceName> rscSet, ApiCallRc responses);
    void rscUpdateApplied(Set<Resource.ResourceKey> rscSet);
    void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet);

    void markResourceForDispatch(ResourceName name);
    void markMultipleResourcesForDispatch(Set<ResourceName> rscSet);

    void fullSyncApplied(Node localNode) throws StorageException;

    void abortDeviceHandlers();

    StltUpdateTracker getUpdateTracker();
    void forceWakeUpdateNotifications();

    SpaceInfo getSpaceInfo(StorPool storPoolRef) throws StorageException;
}
