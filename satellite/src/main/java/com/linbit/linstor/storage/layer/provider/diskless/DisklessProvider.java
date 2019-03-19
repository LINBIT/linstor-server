package com.linbit.linstor.storage.layer.provider.diskless;

import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class DisklessProvider implements DeviceProvider
{
    @Inject
    public DisklessProvider()
    {
        // this class definitely needs dependency injection!
    }

    @Override
    public void clearCache()
    {
        // no-op
    }

    @Override
    public void prepare(List<VlmProviderObject> vlmDataList, List<SnapshotVolume> snapVlms)
    {
        // no-op
    }

    @Override
    public void updateGrossSize(VlmProviderObject vlmObj)
    {
        // no-op
    }

    @Override
    public void updateAllocatedSize(VlmProviderObject vlmDataRef)
        throws AccessDeniedException, SQLException, StorageException
    {
        // no-op
    }

    @Override
    public void process(List<VlmProviderObject> vlmDataList, List<SnapshotVolume> list, ApiCallRcImpl apiCallRc)
    {
        // no-op
    }

    @Override
    public long getPoolCapacity(StorPool storPool)
    {
        return Long.MAX_VALUE;
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool)
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void checkConfig(StorPool storPool)
    {
        // no-op
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        // no-op
    }

    @Override
    public Collection<StorPool> getChangedStorPools()
    {
        return Collections.emptyList();
    }
}
