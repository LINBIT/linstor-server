package com.linbit.linstor.storage.layer.kinds;

public class StorageLayerKind implements DeviceLayerKind
{
    @Override
    public boolean isSnapshotSupported()
    {
        return false;
    }

    @Override
    public boolean isResizeSupported()
    {
        return true;
    }

    @Override
    public StartupVerifications[] requiredVerifications()
    {
        return new StartupVerifications[] {};
    }
}
