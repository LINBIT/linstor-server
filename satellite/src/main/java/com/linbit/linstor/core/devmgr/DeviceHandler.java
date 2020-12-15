package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.DeviceLayer.LayerProcessResult;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Collection;
import java.util.List;

public interface DeviceHandler
{
    void dispatchResources(
        Collection<Resource> rscs,
        Collection<Snapshot> snapshots
    );

    LayerProcessResult process(
        AbsRscLayerObject<Resource> rscLayerData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
        DatabaseException;

    /**
     * @see DeviceLayer#updateAllocatedSizeFromUsableSize(VlmProviderObject)
     *
     * @param vlmData
     *
     * @throws AccessDeniedException
     * @throws DatabaseException
     * @throws StorageException
     */
    void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException;

    /**
     * @see DeviceLayer#updateUsableSizeFromAllocatedSize(VlmProviderObject)
     *
     * @param vlmData
     *
     * @throws AccessDeniedException
     * @throws DatabaseException
     * @throws StorageException
     */
    void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException;

    void sendResourceCreatedEvent(AbsRscLayerObject<Resource> layerDataRef, ResourceState resourceStateRef);

    void sendResourceDeletedEvent(AbsRscLayerObject<Resource> layerDataRef);

    void localNodePropsChanged(Props propsRef) throws StorageException, AccessDeniedException;

    void fullSyncApplied(Node localNodeRef) throws StorageException;

    SpaceInfo getSpaceInfo(StorPool storPoolRef) throws StorageException;

    /**
     * Method called right from within the DeviceManagerImpl's constructor - i.e. at a time
     * where the DeviceManager is already accessible through the injected Provider<DeviceManager>
     */
    default void initialize()
    {}
}
