package com.linbit.linstor.storage.layer;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.devmgr.DeviceHandlerImpl;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;

import java.util.Collection;
import java.util.Map;

/**
 * <p>
 * Base interface for all kinds of DeviceLayers providing or adapting a device.
 * </p>
 * <p>
 * Examples for device providers are LVM, ZFS, their *Thin types, ...
 * Device adaptor are for example DRBD, CryptSetup, ...
 * </p>
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface DeviceLayer
{
    String INTERNAL_STORAGE_NAMESPACE = InternalApiConsts.NAMESPC_INTERNAL + "/" + ApiConsts.NAMESPC_STORAGE_DRIVER;
    String STOR_DRIVER_NAMESPACE = ApiConsts.NAMESPC_STORAGE_DRIVER;

    Map<Resource, StorageException> adjustTopDown(Collection<Resource> resources, Collection<Snapshot> snapshots)
        throws StorageException;

    Map<Resource, StorageException> adjustBottomUp(Collection<Resource> resources, Collection<Snapshot> snapshots)
        throws StorageException;

    void adjust(Resource rsc, DeviceHandlerImpl deviceHandlerImpl)
        throws ResourceException, VolumeException, StorageException;

    void setLocalNodeProps(Props localNodeProps);

    interface NotificationListener
    {
        void notifyResourceDispatchResponse(ResourceName resourceName, ApiCallRc response);

        void notifyResourceApplied(Resource rsc);

        void notifyDrbdVolumeResized(Volume vlm);

        void notifyResourceDeleted(Resource rsc);

        void notifyVolumeDeleted(Volume vlm, long freeSpace);

        void notifySnapshotDeleted(Snapshot snapshot);
    }
}
