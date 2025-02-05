package com.linbit.linstor.layer.storage.diskless;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.storage.DeviceProvider;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.DISKLESS;
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
    public void updateAllocatedSize(VlmProviderObject<Resource> vlmDataRef)
    {
        // no-op
    }

    @Override
    public void processVolumes(
        List<VlmProviderObject<Resource>> vlmDataListRef,
        ApiCallRcImpl apiCallRcRef
    )
    {
        // no-op
    }

    @Override
    public void processSnapshotVolumes(List<VlmProviderObject<Snapshot>> snapVlmDataListRef, ApiCallRcImpl apiCallRcRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        // no-op
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPoolRef) throws AccessDeniedException, StorageException
    {
        return DEFAULT_DISKLESS_SPACE_INFO;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPool)
    {
        // no-op
        return null;
    }

    @Override
    public @Nullable LocalPropsChangePojo setLocalNodeProps(ReadOnlyProps localNodePropsRef)
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
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
    {
        // no-op
        return null;
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }
}
