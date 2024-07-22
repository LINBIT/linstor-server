package com.linbit.linstor.core;

import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;

import java.util.List;
import java.util.Set;

public interface DeviceManager extends DrbdStateChange, DeviceLayer.NotificationListener
{
    void controllerUpdateApplied(Set<ResourceName> rscSet);
    void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet);
    void storPoolUpdateApplied(Set<StorPoolName> storPoolSet, Set<ResourceName> rscSet, ApiCallRc responses);
    void rscUpdateApplied(Set<Resource.ResourceKey> rscSet);
    void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet);
    void externalFileUpdateApplied(ExternalFileName extFile, NodeName nodeName, Set<ResourceName> rscNameSet);

    void remoteUpdateApplied(RemoteName remoteNameRef, NodeName nodeNameRef);

    void markResourceForDispatch(ResourceName name);
    void markMultipleResourcesForDispatch(Set<ResourceName> rscSet);

    void applyChangedNodeProps(Props propsRef) throws StorageException, AccessDeniedException;
    void fullSyncApplied(Node localNode) throws StorageException;

    void abortDeviceHandlers();

    StltUpdateTracker getUpdateTracker();
    void forceWakeUpdateNotifications();

    SpaceInfo getSpaceInfo(StorPoolInfo storPoolInfoRef, boolean update) throws StorageException;

    void sharedStorPoolLocksGranted(List<String> sharedStorPoolLocksListRef);

    void controllerConnectionLost();
    boolean hasAllSharedLocksGranted();
    void registerSharedExtCmdFactory(ExtCmdFactoryStlt extCmdFactoryStltRef);

    StltReadOnlyInfo getReadOnlyData();
    void clearReadOnlyStltInfo();
}
