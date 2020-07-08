package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.openflex.OpenflexLayer;
import com.linbit.linstor.layer.storage.StorageLayer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DeviceLayer
{
    String STOR_DRIVER_NAMESPACE = ApiConsts.NAMESPC_STORAGE_DRIVER;

    String getName();

    void prepare(
        Set<AbsRscLayerObject<Resource>> rscObjList,
        Set<AbsRscLayerObject<Snapshot>> snapObjList
    )
        throws StorageException, AccessDeniedException, DatabaseException;

    /**
     * The layer implementing this method can assume that its usable size is already set.
     * The implementation is expected to update its own allocated size.
     * The implementation is expected to call an update*Size method for the next layer
     *
     * @param vlmData
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException;

    /**
     * The layer implementing this method can assume that its allocated size is already set.
     * The implementation is expected to update its own usable size.
     * The implementation might lower its allocated size if the layer below provides less usable size.
     * The implementation is expected to call an update*Size method for the next layer
     *
     * @param vlmData
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException;

    /**
     * @param rscLayerData The current layer's data. This is an explicit parameter in case we
     * want to (in far future) allow multiple occurrences of the same layer in a given layerStack
     * (could be useful in case of RAID)
     * @param apiCallRc Responses to the ApiCall
     *
     * @throws StorageException
     * @throws ResourceException
     * @throws VolumeException
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    LayerProcessResult process(
        AbsRscLayerObject<Resource> rscLayerData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
        DatabaseException, AbortLayerProcessingException;

    void clearCache() throws StorageException;

    void setLocalNodeProps(Props localNodeProps);

    boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef) throws AccessDeniedException;

    /**
     * Most layers will no-op. Current exceptions are {@link StorageLayer} and {@link OpenflexLayer}
     *
     * @param storPoolRef
     *
     * @throws DatabaseException
     * @throws AccessDeniedException
     * @throws StorageException
     */
    default void checkStorPool(StorPool storPoolRef) throws StorageException, AccessDeniedException, DatabaseException
    {
        // noop
    }

    /**
     * This method should only be implemented by layers managing storage.
     * Those are currently {@link StorageLayer} and {@link OpenflexLayer}
     *
     * @throws StorageException
     * @throws AccessDeniedException
     */
    default SpaceInfo getStoragePoolSpaceInfo(StorPool storPoolRef) throws AccessDeniedException, StorageException
    {
        throw new ImplementationError(
            "Layer " + getClass().getSimpleName() + " does not directly handle (free / total) space information!"
        );
    }

    public interface NotificationListener
    {
        void notifyResourceDispatchResponse(ResourceName resourceName, ApiCallRc response);

        void notifyResourceApplied(Resource rsc);

        void notifyDrbdVolumeResized(Volume vlm);

        void notifyResourceDeleted(Resource rsc);

        void notifyVolumeDeleted(Volume vlm);

        void notifySnapshotDeleted(Snapshot snapshot);

        void notifyFreeSpacesChanged(Map<StorPool, SpaceInfo> spaceInfoMapRef);

        void notifyResourceFailed(Resource rsc, ApiCallRc apiCallRc);
    }

    public enum LayerProcessResult
    {
        SUCCESS, NO_DEVICES_PROVIDED
    }

    public class AbortLayerProcessingException extends LinStorRuntimeException
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
