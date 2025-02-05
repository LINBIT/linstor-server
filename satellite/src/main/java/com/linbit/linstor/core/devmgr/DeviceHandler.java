package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Collection;

public interface DeviceHandler
{
    void dispatchResources(
        Collection<Resource> rscs,
        Collection<Snapshot> snapshots
    );

    void processResource(
        AbsRscLayerObject<Resource> rscLayerData,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
        DatabaseException;

    void processSnapshot(
        AbsRscLayerObject<Snapshot> snapLayerData,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
        DatabaseException;

    void sendResourceCreatedEvent(AbsRscLayerObject<Resource> layerDataRef, ResourceState resourceStateRef);

    void sendResourceDeletedEvent(AbsRscLayerObject<Resource> layerDataRef);

    void localNodePropsChanged(Props propsRef) throws StorageException, AccessDeniedException;

    void fullSyncApplied(Node localNodeRef) throws StorageException;

    SpaceInfo getSpaceInfo(StorPoolInfo storPoolInfoRef, boolean update) throws StorageException;

    /**
     * Method called right from within the DeviceManagerImpl's constructor - i.e. at a time
     * where the DeviceManager is already accessible through the injected Provider<DeviceManager>
     */
    default void initialize()
    {
    }

    enum CloneStrategy
    {
        LVM_THIN_CLONE(0, false), // lvm snapshot, rename and pretend it is a volume
        ZFS_CLONE(0, false), // zfs clone
        ZFS_COPY(10, true), // zfs send/recv
        DD(90, true);

        private final int priority;
        private final boolean needsOpenDevices;

        CloneStrategy(int priorityRef, boolean needsOpenDevicesRef)
        {
            priority = priorityRef;
            needsOpenDevices = needsOpenDevicesRef;
        }

        public int getPriority()
        {
            return priority;
        }

        public boolean needsOpenDevices()
        {
            return needsOpenDevices;
        }

        public static CloneStrategy highestPriority()
        {
            return CloneStrategy.DD;
        }

        public static CloneStrategy maxStrategy(CloneStrategy first, CloneStrategy second)
        {
            return first.getPriority() > second.getPriority() ? first : second;
        }
    }

    void openForClone(VlmProviderObject<?> sourceVlmData, @Nullable String targetRscNameRef)
        throws StorageException, AccessDeniedException, DatabaseException;

    void closeAfterClone(VlmProviderObject<?> vlmDataRef, @Nullable String targetRscNameRef) throws StorageException;

    /**
     * Recursively calls all child layers processAfterClone method that should make any post clone processing needed.
     * @param vlmSrcDataRef
     * @param vlmTgtDataRef
     * @param clonedPath
     * @throws StorageException
     */
    void processAfterClone(VlmProviderObject<?> vlmSrcDataRef, VlmProviderObject<?> vlmTgtDataRef, String clonedPath)
        throws StorageException;
}
