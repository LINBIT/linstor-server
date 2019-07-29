package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;

import java.util.Collection;

public interface DeviceHandler
{
    void dispatchResources(
        Collection<Resource> rscs,
        Collection<Snapshot> snapshots
    );

    void process(
        RscLayerObject rscLayerData,
        Collection<Snapshot> snapshots,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
        DatabaseException;

    void sendResourceCreatedEvent(RscLayerObject layerDataRef, UsageState usageStateRef);

    void sendResourceDeletedEvent(RscLayerObject layerDataRef);
}
