package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.ImplementationError;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;

import java.util.List;
import java.util.Map;

public class SwordfishTargetProvider extends AbsSwordfishProvider
{
    public SwordfishTargetProvider(NotificationListener notificationListenerRef)
    {
        super(notificationListenerRef);
    }

    @Override
    public Map<Volume, StorageException> adjust(List<Volume> volumes)
        throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
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

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }
}
