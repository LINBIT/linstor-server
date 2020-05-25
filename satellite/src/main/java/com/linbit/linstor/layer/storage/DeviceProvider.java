package com.linbit.linstor.layer.storage;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Collection;
import java.util.List;

public interface DeviceProvider
{
    String STORAGE_NAMESPACE = InternalApiConsts.NAMESPC_INTERNAL + "/" + ApiConsts.NAMESPC_STORAGE_DRIVER;

    void clearCache() throws StorageException;

    void prepare(List<VlmProviderObject<Resource>> vlmDataList, List<VlmProviderObject<Snapshot>> snapVlms)
        throws StorageException, AccessDeniedException, DatabaseException;

    void process(
        List<VlmProviderObject<Resource>> vlmDataList,
        List<VlmProviderObject<Snapshot>> snapVlmDataList,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, DatabaseException, StorageException;

    /**
     * @return an instance of {@link SpaceInfo} containing the total capacity as well as the currently free space
     * of the given storage pool
     *
     * @throws StorageException
     * @throws AccessDeniedException
     */
    SpaceInfo getSpaceInfo(StorPool storPoolRef)
        throws AccessDeniedException, StorageException;

    /**
     * Checks if the given {@link StorPool} has a valid configuration for all involved {@link DeviceLayer}s.
     *
     * @param config
     *
     * @throws StorageException
     * @throws AccessDeniedException
     */
    void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException;

    void setLocalNodeProps(Props localNodePropsRef);

    Collection<StorPool> getChangedStorPools();

    void updateGrossSize(VlmProviderObject<Resource> vlmObj)
        throws AccessDeniedException, DatabaseException;

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
    void update(StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException, StorageException;
}
