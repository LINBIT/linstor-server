package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.ReadOnlyOpener;
import com.linbit.linstor.layer.ReadOnlySnapshotHandle;
import com.linbit.linstor.propscon.Props;
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
        throws StorageException, ResourceException, VolumeException, DatabaseException;

    void processSnapshot(
        AbsRscLayerObject<Snapshot> snapLayerData,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, DatabaseException;

    void sendResourceCreatedEvent(AbsRscLayerObject<Resource> layerDataRef, ResourceState resourceStateRef);

    void sendResourceDeletedEvent(AbsRscLayerObject<Resource> layerDataRef);

    void localNodePropsChanged(Props propsRef) throws StorageException;

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

        /**
         * Whether the clone source and target have to be opened before the clone ({@link #openSnapshotReadOnly} with
         * a {@link ReadOnlyOpener.ForClone} and {@link #openCloneTarget}), unlike for the strategies that clone
         * within the storage pool.
         */
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

    /**
     * Creates (if needed) and opens the target volume of a clone for writing, including all layers below the given
     * volume data, and sets the writable device as {@link VlmProviderObject#getCloneDevicePath()} of the given volume
     * data. The source of a clone is opened with {@link #openSnapshotReadOnly} and a
     * {@link ReadOnlyOpener.ForClone}.
     */
    void openCloneTarget(VlmProviderObject<Resource> tgtVlmData) throws StorageException, DatabaseException;

    /**
     * Closes what {@link #openCloneTarget} opened.
     */
    void closeCloneTarget(VlmProviderObject<Resource> tgtVlmData) throws StorageException;

    /**
     * Opens a snapshot read-only, including all layers below the opener's volume data, and sets the readable device as
     * {@link VlmProviderObject#getCloneDevicePath()} of that volume data. What gets opened depends on the opener:
     * <ul>
     *  <li>{@link ReadOnlyOpener.ForSnapshot}: the LINSTOR snapshot itself.</li>
     *  <li>{@link ReadOnlyOpener.ForClone}: the clone snapshot that was taken of the clone source for
     *      {@link ReadOnlyOpener.ForClone#cloneTargetName()}.</li>
     * </ul>
     * Not every layer supports this (e.g. DRBD does not): callers start below such layers.
     * <p>
     * Several openers can have the same snapshot (or snapshots sharing device state, like ZFS snapshots of the same
     * zvol) open at the same time: shared device state is only reverted when its last opener closes.
     * <p>
     * Unlike {@link #openCloneTarget} / {@link #closeCloneTarget}, there is no method to close the snapshot again:
     * the caller is expected to close the returned (auto-closeable) handle, ideally in a try-with-resources block, or
     * otherwise make sure that {@link ReadOnlySnapshotHandle#close()} gets called (e.g. by handing the handle on to
     * whoever finishes using the snapshot). An unclosed handle leaves device mappings open and activated snapshots
     * (or visible ZFS snapshot devices) in place.
     *
     * @return the handle that closes the snapshot again; closing it more than once has no effect
     */
    <RSC extends AbsResource<RSC>> ReadOnlySnapshotHandle openSnapshotReadOnly(ReadOnlyOpener<RSC> opener)
        throws StorageException, DatabaseException;

    /**
     * Recursively calls all child layers processAfterClone method that should make any post clone processing needed.
     */
    void processAfterClone(VlmProviderObject<?> vlmSrcDataRef, VlmProviderObject<?> vlmTgtDataRef, String clonedPath)
        throws StorageException;
}
