package com.linbit.linstor.storage.layer;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface DeviceLayer
{
    String STOR_DRIVER_NAMESPACE = ApiConsts.NAMESPC_STORAGE_DRIVER;

    String getName();

    void prepare(Set<RscLayerObject> rscDataList, Set<Snapshot> affectedSnapshots)
        throws StorageException, AccessDeniedException, DatabaseException;

    void updateGrossSize(VlmProviderObject vlmData)
        throws AccessDeniedException, DatabaseException;

    /**
     * @param rsc The resource to process
     * @param rscLayerData The current layer's data. This is an explicit parameter in case we
     * want to (in far future) allow multiple occurrences of the same layer in a given layerStack
     * (could be useful in case of RAID)
     * @param rscNameSuffix A suffix (can be "") which will be appended to the resource name, but
     * before the volume numbers in case of storage-layers. This is necessary for the RAID layer
     * @param snapshots Snapshots to be processed
     * @param apiCallRc Responses to the ApiCall
     *
     * @throws StorageException
     * @throws ResourceException
     * @throws VolumeException
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    LayerProcessResult process(
        RscLayerObject rscLayerData,
        Collection<Snapshot> snapshots,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
            DatabaseException;

    void clearCache() throws StorageException;

    void setLocalNodeProps(Props localNodeProps);

    void resourceFinished(RscLayerObject layerDataRef) throws AccessDeniedException;

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
}
