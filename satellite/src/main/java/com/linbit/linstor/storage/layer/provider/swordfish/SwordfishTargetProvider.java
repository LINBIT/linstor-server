package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.ImplementationError;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;

@Singleton
public class SwordfishTargetProvider extends AbsSwordfishProvider
{
    @Inject
    public SwordfishTargetProvider(
        Provider<NotificationListener> notificationListenerProvider
    )
    {
        super(notificationListenerProvider, "SFI", "created", "deleted");
    }

    @Override
    public void process(List<Volume> volumes, List<SnapshotVolume> snapVolumes, ApiCallRcImpl apiCallRc)
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
