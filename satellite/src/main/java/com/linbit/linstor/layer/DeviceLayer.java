package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.SuspendManager;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.identifier.ResourceName;
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
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;

import java.util.Map;
import java.util.Set;

public interface DeviceLayer
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
        throws StorageException, AccessDeniedException, DatabaseException;

    /**
     * @param rscLayerData The current layer's data. This is an explicit parameter in case we
     *     want to (in far future) allow multiple occurrences of the same layer in a given layerStack
     *     (could be useful in case of RAID)
     * @param apiCallRc Responses to the ApiCall
     *
     * @throws StorageException
     * @throws ResourceException
     * @throws VolumeException
     * @throws AccessDeniedException
     * @throws DatabaseException
     * @throws AbortLayerProcessingException
     */
    void processResource(AbsRscLayerObject<Resource> rscLayerData, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException,
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
     * @throws StorageException
     * @throws ResourceException
     * @throws VolumeException
     * @throws AccessDeniedException
     * @throws DatabaseException
     * @throws AbortLayerProcessingException
     */
    default boolean processSnapshot(AbsRscLayerObject<Snapshot> snapLayerData, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
        DatabaseException, AbortLayerProcessingException
    {
        // noop
        return false;
    }

    void clearCache() throws StorageException;

    LocalPropsChangePojo setLocalNodeProps(Props localNodeProps) throws StorageException, AccessDeniedException;

    boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef) throws AccessDeniedException;

    /**
     * @return true if this layer supports suspend, false otherwise
     */
    boolean isSuspendIoSupported();

    /**
     * @param rscDataRef
     *
     * @return if this layer needs to run some extra code after its parent layer has already suspended IO
     *
     * @throws StorageException
     */
    default void managePostRootSuspend(AbsRscLayerObject<Resource> rscDataRef) throws StorageException
    {
        // noop
    }

    /**
     * The layer is expected to update the given rscData's {@link AbsRscLayerObject#setIsSuspended(boolean)} state so
     * that the {@link SuspendManager} can properly check if a {@link #suspendIo(AbsRscLayerObject)} or
     * {@link #resumeIo(AbsRscLayerObject)} call is needed
     *
     * @throws StorageException
     * @throws DatabaseException
     * @throws ExtCmdFailedException
     */
    void updateSuspendState(AbsRscLayerObject<Resource> rscDataRef)
        throws StorageException, DatabaseException, ExtCmdFailedException;

    /*
     * Performs a suspend-io
     */
    void suspendIo(AbsRscLayerObject<Resource> rscDataRef) throws ExtCmdFailedException, StorageException;

    /*
     * Performs a resume-io
     */
    void resumeIo(AbsRscLayerObject<Resource> rscDataRef) throws ExtCmdFailedException, StorageException;

    /**
     * Returns whether or not checking for 'lsblk ... -o DISC-GRAN' makes sense for this RscData.
     * For example it does not make sense for DRBD diskless
     *
     * @throws AccessDeniedException
     */
    default boolean isDiscGranFeasible(AbsRscLayerObject<Resource> rscLayerObjectRef) throws AccessDeniedException
    {
        return true;
    }

    /**
     * Most layers will no-op. Current exceptions is {@link StorageLayer}
     *
     * @param storPoolInfoRef
     * @param update
     *
     * @throws DatabaseException
     * @throws AccessDeniedException
     * @throws StorageException
     */
    default LocalPropsChangePojo checkStorPool(StorPoolInfo storPoolInfoRef, boolean update)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // no-op, no change in props
        return null;
    }

    /**
     * This method should only be implemented by layers managing storage.
     * Which currently is only {@link StorageLayer}
     *
     * @param storPoolInfoRef
     *
     * @throws StorageException
     * @throws AccessDeniedException
     */
    default SpaceInfo getStoragePoolSpaceInfo(StorPoolInfo storPoolInfoRef)
        throws AccessDeniedException, StorageException
    {
        throw new ImplementationError(
            "Layer " + getClass().getSimpleName() + " does not directly handle (free / total) space information!"
        );
    }

    public interface NotificationListener
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
}
