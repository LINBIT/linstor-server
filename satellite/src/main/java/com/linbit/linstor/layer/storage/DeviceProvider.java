package com.linbit.linstor.layer.storage;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessDeniedException;
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
        throws StorageException, AccessDeniedException, DatabaseException;

    void processVolumes(List<VlmProviderObject<Resource>> vlmDataList, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, DatabaseException, StorageException;

    void processSnapshotVolumes(List<VlmProviderObject<Snapshot>> snapVlmDataList, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, DatabaseException, StorageException;

    /**
     * @return an instance of {@link SpaceInfo} containing the total capacity as well as the currently free space
     * of the given storage pool
     *
     * @throws StorageException
     * @throws AccessDeniedException
     */
    SpaceInfo getSpaceInfo(StorPoolInfo roStorPoolRef) throws AccessDeniedException, StorageException;

    /**
     * Checks if the given {@link StorPool} has a valid configuration for all involved {@link DeviceLayer}s.
     *
     * @param config
     *
     * @return
     *
     * @throws StorageException
     * @throws AccessDeniedException
     */
    LocalPropsChangePojo checkConfig(StorPoolInfo storPool) throws StorageException, AccessDeniedException;

    LocalPropsChangePojo setLocalNodeProps(ReadOnlyProps localNodePropsRef)
        throws StorageException, AccessDeniedException;

    Collection<StorPool> getChangedStorPools();

    void updateAllocatedSize(VlmProviderObject<Resource> vlmObj)
        throws AccessDeniedException, DatabaseException, StorageException;

    /**
     * Used to determine properties of the storage pool, i.e. if the storage pool is ontop of a pmem device
     *
     * @param storPoolRef
     * @throws AccessDeniedException
     * @throws DatabaseException
     * @throws StorageException
     */
    @Nullable
    LocalPropsChangePojo update(StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException, StorageException;

    DeviceProviderKind getDeviceProviderKind();

    Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException, AccessDeniedException;

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

    default void openForClone(VlmProviderObject<?> vlm, @Nullable String clonename, boolean readOnly)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("openForClone not suppported.");
    }

    // TODO probably need a way to decide if snapshot path needs to be closed
    default void closeForClone(VlmProviderObject<?> vlm, @Nullable String cloneName) throws StorageException
    {
        throw new StorageException("closeForClone not supported.");
    }
}
