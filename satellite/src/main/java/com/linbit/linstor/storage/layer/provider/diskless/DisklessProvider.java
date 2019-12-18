package com.linbit.linstor.storage.layer.provider.diskless;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;

import javax.inject.Inject;
import javax.inject.Singleton;

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
    public void prepare(List<VlmProviderObject<Resource>> vlmDataListRef, List<VlmProviderObject<Snapshot>> snapVlmsRef)
    {
        // no-op
    }

    @Override
    public void updateGrossSize(VlmProviderObject<Resource> vlmObj)
    {
        // no-op
    }

    @Override
    public void updateAllocatedSize(VlmProviderObject<Resource> vlmDataRef)
    {
        // no-op
    }

    @Override
    public void process(
        List<VlmProviderObject<Resource>> vlmDataListRef,
        List<VlmProviderObject<Snapshot>> snapVlmDataListRef,
        ApiCallRcImpl apiCallRcRef
    )
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

    @Override
    public void update(StorPool storPoolRef)
    {
        // no-op
    }
}
