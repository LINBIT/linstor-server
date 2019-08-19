package com.linbit.linstor.storage.kinds;

public enum DeviceLayerKind
{
    DRBD(
        false,
        StartupVerification.DRBD9
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
        StartupVerification.CRYPT_SETUP
    ),
    NVME(
        false,
        StartupVerification.NVME
    ),
    STORAGE(true);
    private final StartupVerification[] startupVerifications;

    private boolean localOnly;

    DeviceLayerKind(
        boolean localOnlyRef,
        StartupVerification... startupVerificationsRef
    )
    {
        startupVerifications = startupVerificationsRef;
        localOnly = localOnlyRef;
    }

    public StartupVerification[] getStartupVerifications()
    {
        return startupVerifications;
    }

    public boolean isLocalOnly()
    {
        return localOnly;
    }
}
