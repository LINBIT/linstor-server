package com.linbit.linstor.layer.storage.diskless;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.layer.storage.DeviceProvider;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class DisklessProvider implements DeviceProvider
{
    private static final SpaceInfo DEFAULT_DISKLESS_SPACE_INFO;

    static
    {
        DEFAULT_DISKLESS_SPACE_INFO = new SpaceInfo(Long.MAX_VALUE, Long.MAX_VALUE);
    }

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
    public SpaceInfo getSpaceInfo(StorPool storPoolRef) throws AccessDeniedException, StorageException
    {
        return DEFAULT_DISKLESS_SPACE_INFO;
    }

    @Override
    public LocalPropsChangePojo checkConfig(StorPool storPool)
    {
        // no-op
        return null;
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(Props localNodePropsRef)
    {
        // no-op
        return null;
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
