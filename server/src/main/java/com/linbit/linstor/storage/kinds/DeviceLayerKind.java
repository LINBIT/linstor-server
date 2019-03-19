package com.linbit.linstor.storage.kinds;

public enum DeviceLayerKind
{
    DRBD(
        StartupVerifications.UNAME, StartupVerifications.DRBD9
    ),
    //    DRBD_PROXY(
    //        false,
    //        true,
    //        false,
    //        false,
    //        StartupVerifications.DRBD_PROXY
    //    ),
    LUKS(
        StartupVerifications.CRYPT_SETUP
    ),
    STORAGE();
    private final StartupVerifications[] startupVerifications;

    DeviceLayerKind(
        StartupVerifications... startupVerificationsRef
    )
    {
        startupVerifications = startupVerificationsRef;
    }

    public StartupVerifications[] getStartupVerifications()
    {
        return startupVerifications;
    }
}
