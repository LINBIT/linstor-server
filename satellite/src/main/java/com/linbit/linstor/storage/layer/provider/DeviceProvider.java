package com.linbit.linstor.storage.layer.provider;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public interface DeviceProvider
{
    String STORAGE_NAMESPACE = InternalApiConsts.NAMESPC_INTERNAL + "/" + ApiConsts.NAMESPC_STORAGE_DRIVER;

    void clearCache() throws StorageException;

    void prepare(List<VlmProviderObject> vlmDataList, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException;

    void process(
        List<VlmProviderObject> vlmDataList,
        List<SnapshotVolume> list,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, SQLException, StorageException;

    /**
     * @return the capacity of the used storage pool.
     * @throws StorageException
     * @throws AccessDeniedException
     */
    long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException;

    /**
     * @return the free space of the used storage pool.
     * @throws StorageException
     * @throws AccessDeniedException
     */
    long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException;

    /**
     * Checks if the given {@link StorPool} has a valid configuration for all involved {@link DeviceLayer}s.
     *
     * @param config
     * @throws StorageException
     * @throws AccessDeniedException
     */
    void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException;

    void setLocalNodeProps(Props localNodePropsRef);

    Collection<StorPool> getChangedStorPools();

    void updateGrossSize(VlmProviderObject vlmObj)
        throws AccessDeniedException, SQLException;

    void updateAllocatedSize(VlmProviderObject vlmObj)
        throws AccessDeniedException, SQLException, StorageException;
}
