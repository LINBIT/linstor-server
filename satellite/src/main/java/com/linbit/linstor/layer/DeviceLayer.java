package com.linbit.linstor.layer;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.DeviceHandler.CloneStrategy;
import com.linbit.linstor.core.devmgr.SuspendManager;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.storage.StorageLayer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DeviceLayer extends Comparable<DeviceLayer>
{
    String STOR_DRIVER_NAMESPACE = ApiConsts.NAMESPC_STORAGE_DRIVER;

    /**
     * Method called right from within the DeviceManagerImpl's constructor - i.e. at a time
     * where the DeviceManager is already accessible through the injected Provider<DeviceManager>
     */
    default void initialize()
    {
    }

    String getName();

    void prepare(
        Set<AbsRscLayerObject<Resource>> rscObjList,
        Set<AbsRscLayerObject<Snapshot>> snapObjList
    )
        throws StorageException, DatabaseException;

    /**
     * Processes the given resource layer data.
     *
     * @param rscLayerData The current layer's data. This is an explicit parameter in case we
     *     want to (in far future) allow multiple occurrences of the same layer in a given layerStack
     *     (could be useful in case of RAID)
     * @param apiCallRc Responses to the ApiCall
     *
     */
    void processResource(AbsRscLayerObject<Resource> rscLayerData, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, DatabaseException,
        AbortLayerProcessingException;

    /**
     * Unlike the {@link #processResource(AbsRscLayerObject, ApiCallRcImpl)} method which has to make sure to properly
     * call the next layer's processResource method, the processSnapshot method can just return false to indicate that
     * it has not done anything with the snapshot at all, thus passing all of its children objects to the corresponding
     * next layer(s).
     *
     * @param snapLayerData The current layer's data. This is an explicit parameter in case we
     *     want to (in far future) allow multiple occurrences of the same layer in a given layerStack
     *     (could be useful in case of RAID)
     * @param apiCallRc Responses to the ApiCall
     *
     */
    default boolean processSnapshot(AbsRscLayerObject<Snapshot> snapLayerData, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, DatabaseException, AbortLayerProcessingException
    {
        // noop
        return false;
    }

    void clearCache() throws StorageException;

    @Nullable
    LocalPropsChangePojo setLocalNodeProps(Props localNodeProps) throws StorageException;

    boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef);

    /**
     * Returns whether this layer supports suspend IO.
     *
     * @return true if this layer supports suspend, false otherwise
     */
    boolean isSuspendIoSupported();

    /**
     * The layer is expected to update the given rscData's {@link AbsRscLayerObject#setIsSuspended(boolean)} state so
     * that the {@link SuspendManager} can properly check if a {@link #suspendIo(AbsRscLayerObject, boolean)} or
     * {@link #resumeIo(AbsRscLayerObject, boolean)} call is needed
     *
     */
    void updateSuspendState(AbsRscLayerObject<Resource> rscDataRef)
        throws StorageException, DatabaseException, ExtCmdFailedException;

    /*
     * Performs a suspend-io
     */
    void suspendIo(AbsRscLayerObject<Resource> rscDataRef, boolean asRootLayerRef)
        throws ExtCmdFailedException, StorageException, ChildProcessTimeoutException, IOException;

    /*
     * Performs a resume-io
     */
    void resumeIo(AbsRscLayerObject<Resource> rscDataRef, boolean asRootLayerRef)
        throws ExtCmdFailedException, StorageException;

    /**
     * Returns whether or not checking for 'lsblk ... -o DISC-GRAN' makes sense for this RscData.
     * For example it does not make sense for DRBD diskless
     *
     */
    default boolean isDiscGranFeasible(AbsRscLayerObject<Resource> rscLayerObjectRef)
    {
        return true;
    }

    /**
     * Most layers will no-op. Current exceptions is {@link StorageLayer}
     *
     *
     */
    default @Nullable LocalPropsChangePojo checkStorPool(StorPoolInfo storPoolInfoRef, boolean update)
        throws StorageException, DatabaseException
    {
        // no-op, no change in props
        return null;
    }

    /**
     * This method should only be implemented by layers managing storage.
     * Which currently is only {@link StorageLayer}
     *
     *
     */
    default SpaceInfo getStoragePoolSpaceInfo(StorPoolInfo storPoolInfoRef)
        throws StorageException
    {
        throw new ImplementationError(
            "Layer " + getClass().getSimpleName() + " does not directly handle (free / total) space information!"
        );
    }

    interface NotificationListener
    {
        void notifyResourceDispatchResponse(ResourceName resourceName, ApiCallRc response);

        void notifySnapshotDispatchResponse(SnapshotDefinition.Key snapDfnKeyRef, ApiCallRc responseRef);

        void notifyResourceApplied(Resource rsc);

        void notifyDrbdVolumeResized(Volume vlm);

        void notifyResourceDeleted(Resource rsc);

        void notifyVolumeDeleted(Volume vlm);

        void notifySnapshotDeleted(Snapshot snapshot);

        void notifyFreeSpacesChanged(Map<StorPool, SpaceInfo> spaceInfoMapRef);

        void notifyResourceFailed(Resource rsc, ApiCallRc apiCallRc);

        void notifySnapshotRollbackResult(Resource rscRef, ApiCallRc apiCallRcRef, boolean successRef);
    }

    class AbortLayerProcessingException extends LinStorRuntimeException
    {
        private static final long serialVersionUID = -3885415188860635819L;
        public final AbsRscLayerObject<?> rscLayerObject;

        public AbortLayerProcessingException(AbsRscLayerObject<?> rscLayerObjectRef)
        {
            super(
                "Layer '" + rscLayerObjectRef.getLayerKind().name() + "' aborted by failed " +
                    (rscLayerObjectRef.getAbsResource() instanceof Resource ?
                        "resource '" + rscLayerObjectRef.getSuffixedResourceName() :
                        "snapshot '" + ((Snapshot) rscLayerObjectRef.getAbsResource()).getSnapshotName().displayValue +
                            "' of resource '" + rscLayerObjectRef.getSuffixedResourceName())
            );
            rscLayerObject = rscLayerObjectRef;

        }

        public AbortLayerProcessingException(AbsRscLayerObject<?> rscLayerObjectRef, String stringRef)
        {
            super(stringRef);
            rscLayerObject = rscLayerObjectRef;
        }
    }

    class BlockedPortsException extends AbortLayerProcessingException
    {
        private static final long serialVersionUID = -9127363761130307850L;
        private final List<Integer> blockedPorts;
        private final long retCode;

        public BlockedPortsException(
            AbsRscLayerObject<?> rscLayerObjectRef,
            List<Integer> blockedPortsRef,
            long retCodeRef
        )
        {
            super(
                rscLayerObjectRef,
                "Layer '" + rscLayerObjectRef.getLayerKind().name() + "' aborted due to blocked ports (" +
                    blockedPortsRef + ") for " +
                    (rscLayerObjectRef.getAbsResource() instanceof Resource ?
                        "resource '" + rscLayerObjectRef.getSuffixedResourceName() :
                        "snapshot '" + ((Snapshot) rscLayerObjectRef.getAbsResource()).getSnapshotName().displayValue +
                            "' of resource '" + rscLayerObjectRef.getSuffixedResourceName())
            );
            blockedPorts = blockedPortsRef;
            retCode = retCodeRef;
        }

        public List<Integer> getBlockedPorts()
        {
            return blockedPorts;
        }

        public long getRetCode()
        {
            return retCode;
        }
    }

    boolean isDeleteFlagSet(AbsRscLayerObject<?> rscDataRef);

    enum CloneSupportResult
    {
        TRUE,
        FALSE,
        PASSTHROUGH
    }

    CloneSupportResult getCloneSupport(
        AbsRscLayerObject<?> sourceRscLayerObjectRef,
        AbsRscLayerObject<?> targetRscLayerObjectRef
    );

    default Set<CloneStrategy> getCloneStrategy(VlmProviderObject<?> vlm)
        throws StorageException
    {
        return Collections.emptySet();
    }

    /**
     * Creates (if needed) and opens the target volume of a clone for writing and sets its
     * {@link VlmProviderObject#getCloneDevicePath()}, see
     * {@link com.linbit.linstor.core.devmgr.DeviceHandler#openCloneTarget}. Layers below are opened through the device
     * handler.
     */
    default void openCloneTarget(VlmProviderObject<Resource> vlm)
        throws StorageException, DatabaseException
    {
        throw new StorageException("Layer " + getKind() + " does not support opening a clone target");
    }

    /**
     * Closes what {@link #openCloneTarget} opened, including the layers below, and resets the
     * {@link VlmProviderObject#getCloneDevicePath()}. The target volume itself is kept.
     */
    default void closeCloneTarget(VlmProviderObject<Resource> vlm) throws StorageException
    {
        throw new StorageException("Layer " + getKind() + " does not support closing a clone target");
    }

    /**
     * Opens a snapshot read-only and sets the readable device as {@link VlmProviderObject#getCloneDevicePath()} of the
     * opener's volume data, see {@link com.linbit.linstor.core.devmgr.DeviceHandler#openSnapshotReadOnly}. Layers
     * below are opened through the device handler with {@link ReadOnlyOpener#withVlmData}, and the returned handle
     * closes them as well.
     */
    default <RSC extends AbsResource<RSC>> ReadOnlySnapshotHandle openSnapshotReadOnly(ReadOnlyOpener<RSC> opener)
        throws StorageException, DatabaseException
    {
        throw new StorageException("Layer " + getKind() + " does not support opening a snapshot read-only");
    }

    /**
     * Default post clone processing.
     */
    default void processAfterClone(
        VlmProviderObject<Snapshot> vlmSrc,
        VlmProviderObject<Resource> vlmTgt,
        String clonedPath
    )
        throws StorageException
    {
    }

    DeviceLayerKind getKind();

    @Override
    default int compareTo(DeviceLayer other)
    {
        int cmp = Integer.compare(this.getKind().getOrder(), other.getKind().getOrder());
        if (cmp == 0)
        {
            cmp = this.getKind().name().compareTo(other.getKind().name());
        }
        return cmp;
    }
}
