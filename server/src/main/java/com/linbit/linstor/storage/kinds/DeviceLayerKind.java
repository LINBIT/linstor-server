package com.linbit.linstor.storage.kinds;

public enum DeviceLayerKind
{
    DRBD(
        false,
        ExtTools.DRBD9
    ),
    //    DRBD_PROXY(
    //        false,
    //        true,
    //        false,
    //        false,
    //        StartupVerifications.DRBD_PROXY
    //    ),
    LUKS(
        true,
        ExtTools.CRYPT_SETUP
    ),
    NVME(
        false,
        ExtTools.NVME
    ),
    STORAGE(true);
    private final ExtTools[] startupVerifications;

    private boolean localOnly;

    DeviceLayerKind(
        boolean localOnlyRef,
        ExtTools... startupVerificationsRef
    )
    {
        startupVerifications = startupVerificationsRef;
        localOnly = localOnlyRef;
    }

    public ExtTools[] getExtToolDependencies()
    {
        return startupVerifications;
    }

    public boolean isLocalOnly()
    {
        return localOnly;
    }
}
