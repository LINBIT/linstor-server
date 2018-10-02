package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRc;

import java.util.Set;

public interface DeviceManager extends DrbdStateChange
{
    void controllerUpdateApplied(Set<ResourceName> rscSet);
    void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet);
    void storPoolUpdateApplied(Set<StorPoolName> storPoolSet, Set<ResourceName> rscSet);
    void rscUpdateApplied(Set<Resource.Key> rscSet);
    void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet);

    void notifyResourceDispatchResponse(ResourceName resourceName, ApiCallRc response);
    void notifyResourceApplied(Resource rsc);
    void notifyVolumeResized(Volume vlm);
    void notifyDrbdVolumeResized(Volume vlm);
    void notifyResourceDeleted(Resource rsc);
    void notifyVolumeDeleted(Volume vlm, long freeSpace);
    void notifySnapshotDeleted(Snapshot snapshot);

    void markResourceForDispatch(ResourceName name);
    void markMultipleResourcesForDispatch(Set<ResourceName> rscSet);

    void fullSyncApplied();

    void abortDeviceHandlers();

    StltUpdateTracker getUpdateTracker();
}
