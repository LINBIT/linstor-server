package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ZfsProvider implements DeviceProvider
{
    protected final ExtCmdFactory extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final NotificationListener notificationListener;
    protected Props localNodeProps;

    public ZfsProvider(
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        NotificationListener notificationListenerRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        storDriverAccCtx = storDriverAccCtxRef;
        notificationListener = notificationListenerRef;
    }

    @Override
    public Map<Volume, StorageException> adjust(List<Volume> volumes)
        throws StorageException
    {
        return Collections.emptyMap();
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public void createSnapshot(Volume vlm, String snapshotName) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public void restoreSnapshot(Volume srcVlm, String snapshotName, Volume targetVlm) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public void deleteSnapshot(Volume vlm, String snapshotName) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public boolean snapshotExists(Volume vlm, String snapshotName) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

}
