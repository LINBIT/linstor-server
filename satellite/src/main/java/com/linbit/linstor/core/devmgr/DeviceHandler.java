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
import com.linbit.linstor.event.common.UsageState;
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
     * @param vlmData
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException;

    /**
     * @see DeviceLayer#updateUsableSizeFromAllocatedSize(VlmProviderObject)
     * @param vlmData
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException;

    void sendResourceCreatedEvent(AbsRscLayerObject<Resource> layerDataRef, UsageState usageStateRef);

    void sendResourceDeletedEvent(AbsRscLayerObject<Resource> layerDataRef);

    void localNodePropsChanged(Props propsRef);

    void fullSyncApplied(Node localNodeRef);

    SpaceInfo getSpaceInfo(StorPool storPoolRef) throws StorageException;
}
