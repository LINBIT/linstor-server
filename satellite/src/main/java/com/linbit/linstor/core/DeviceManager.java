package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;

import java.util.Set;

public interface DeviceManager extends DrbdStateChange
{
    void controllerUpdateApplied();
    void nodeUpdateApplied(Set<NodeName> nodeSet);
    void rscDefUpdateApplied(Set<ResourceName> rscDfnSet);
    void storPoolUpdateApplied(Set<StorPoolName> storPoolSet);
    void rscUpdateApplied(Set<Resource.Key> rscSet);
    void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet);

    void notifyResourceApplied(Resource rsc);
    void notifyVolumeResized(Volume vlm);
    void notifyDrbdVolumeResized(Volume vlm);
    void notifyResourceDeleted(Resource rsc);
    void notifyVolumeDeleted(Volume vlm);
    void notifySnapshotDeleted(Snapshot snapshot);

    void fullSyncApplied();

    void abortDeviceHandlers();

    StltUpdateTracker getUpdateTracker();
}
