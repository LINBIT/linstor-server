package com.linbit.linstor.layer.storage;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.ReadOnlyOpener;
import com.linbit.linstor.layer.ReadOnlySnapshotHandle;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface DeviceProvider
{
    String STORAGE_NAMESPACE = InternalApiConsts.NAMESPC_INTERNAL + "/" + ApiConsts.NAMESPC_STORAGE_DRIVER;

    /**
     * Method called right from within the DeviceManagerImpl's constructor - i.e. at a time
     * where the DeviceManager is already accessible through the injected Provider<DeviceManager>
     */
    default void initialize()
    {
    }

    void clearCache() throws StorageException;

    void prepare(List<VlmProviderObject<Resource>> vlmDataList, List<VlmProviderObject<Snapshot>> snapVlms)
        throws StorageException, DatabaseException;

    void processVolumes(List<VlmProviderObject<Resource>> vlmDataList, ApiCallRcImpl apiCallRc)
        throws DatabaseException, StorageException;

    void processSnapshotVolumes(List<VlmProviderObject<Snapshot>> snapVlmDataList, ApiCallRcImpl apiCallRc)
        throws DatabaseException, StorageException;

    /**
     * Returns space information for the given storage pool.
     *
     * @return an instance of {@link SpaceInfo} containing the total capacity as well as the currently free space
     * of the given storage pool
     *
     */
    SpaceInfo getSpaceInfo(StorPoolInfo roStorPoolRef) throws StorageException;

    /**
     * Checks if the given {@link StorPool} has a valid configuration for all involved {@link DeviceLayer}s.
     *
     *
     *
     */
    LocalPropsChangePojo checkConfig(StorPoolInfo storPool) throws StorageException;

    LocalPropsChangePojo setLocalNodeProps(ReadOnlyProps localNodePropsRef)
        throws StorageException;

    Collection<StorPool> getChangedStorPools();

    void updateAllocatedSize(VlmProviderObject<Resource> vlmObj)
        throws DatabaseException, StorageException;

    /**
     * Used to determine properties of the storage pool, i.e. if the storage pool is ontop of a pmem device
     *
     */
    @Nullable
    LocalPropsChangePojo update(StorPool storPoolRef)
        throws DatabaseException, StorageException;

    DeviceProviderKind getDeviceProviderKind();

    Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException;

    default Map<ReadOnlyVlmProviderInfo, Long> fetchOrigAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        Map<ReadOnlyVlmProviderInfo, Long> ret = new HashMap<>();
        for (ReadOnlyVlmProviderInfo roVlmProv : vlmDataListRef)
        {
            ret.put(roVlmProv, roVlmProv.getOrigAllocatedSize());
        }
        return ret;
    }

    /**
     * Creates the target volume of a clone and sets its {@link VlmProviderObject#getCloneDevicePath()}.
     */
    default void openCloneTarget(VlmProviderObject<Resource> vlm)
        throws StorageException, DatabaseException
    {
        throw new StorageException("openCloneTarget not supported.");
    }

    /**
     * Closes what {@link #openCloneTarget} opened and resets the {@link VlmProviderObject#getCloneDevicePath()}. The
     * target volume itself is kept.
     */
    default void closeCloneTarget(VlmProviderObject<Resource> vlm) throws StorageException
    {
        throw new StorageException("closeCloneTarget not supported.");
    }

    /**
     * Makes a storage snapshot readable and sets its device as {@link VlmProviderObject#getCloneDevicePath()} of the
     * opener's volume data: the LINSTOR snapshot for a {@link ReadOnlyOpener.ForSnapshot}, or the clone snapshot of
     * the clone source for a {@link ReadOnlyOpener.ForClone}.
     *
     * @return the handle that makes the snapshot unreadable again (if no other opener needs it) and resets the
     *     clone device path
     */
    default <RSC extends AbsResource<RSC>> ReadOnlySnapshotHandle openSnapshotReadOnly(ReadOnlyOpener<RSC> opener)
        throws StorageException, DatabaseException
    {
        throw new StorageException("openSnapshotReadOnly not supported.");
    }
}
