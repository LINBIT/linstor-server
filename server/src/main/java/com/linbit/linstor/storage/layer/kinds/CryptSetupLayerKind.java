package com.linbit.linstor.storage.layer.kinds;

public class CryptSetupLayerKind implements DeviceLayerKind
{
    @Override
    public boolean isSnapshotSupported()
    {
        return false;
    }

    @Override
    public boolean isResizeSupported()
    {
        return false; // resize IS supported by CryptSetup, but not (yet) implemented by linstor
    }

    @Override
    public StartupVerifications[] requiredVerifications()
    {
        return new StartupVerifications[] {StartupVerifications.CRYPT_SETUP};
    }
}
