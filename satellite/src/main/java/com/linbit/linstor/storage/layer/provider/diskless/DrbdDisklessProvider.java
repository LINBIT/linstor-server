package com.linbit.linstor.storage.layer.provider.diskless;

import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.utils.AccessUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class DrbdDisklessProvider implements DeviceProvider
{
    private AccessContext sysCtx;

    @Inject
    public DrbdDisklessProvider(
        @DeviceManagerContext AccessContext sysCtxRef
    )
    {
        sysCtx = sysCtxRef;
    }

    @Override
    public void clearCache() throws StorageException
    {
        // no-op
    }

    @Override
    public void prepare(List<Volume> volumes, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    public void process(List<Volume> volumes, List<SnapshotVolume> list, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, SQLException, StorageException
    {
        volumes.forEach(vlm -> AccessUtils.execPrivileged(() ->
            {
                vlm.setDevicePath(sysCtx, "none");
                vlm.setAllocatedSize(sysCtx, 0);
                vlm.setUsableSize(sysCtx, Long.MAX_VALUE);
            }
        ));
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return Long.MAX_VALUE;
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
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
