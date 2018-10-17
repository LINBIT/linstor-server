package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.storage.layer.DeviceLayer;

import java.util.Set;

public interface DeviceManager extends DrbdStateChange, DeviceLayer.NotificationListener
{
    void controllerUpdateApplied(Set<ResourceName> rscSet);
    void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet);
    void storPoolUpdateApplied(Set<StorPoolName> storPoolSet, Set<ResourceName> rscSet, ApiCallRc responses);
    void rscUpdateApplied(Set<Resource.Key> rscSet);
    void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet);

    void markResourceForDispatch(ResourceName name);
    void markMultipleResourcesForDispatch(Set<ResourceName> rscSet);

    void fullSyncApplied();

    void abortDeviceHandlers();

    StltUpdateTracker getUpdateTracker();
}
