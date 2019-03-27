package com.linbit.linstor.storage.kinds;

public enum DeviceLayerKind
{
    DRBD(
        StartupVerification.UNAME, StartupVerification.DRBD9
    ),
    //    DRBD_PROXY(
    //        false,
    //        true,
    //        false,
    //        false,
    //        StartupVerifications.DRBD_PROXY
    //    ),
    LUKS(
        StartupVerification.CRYPT_SETUP
    ),
    STORAGE();
    private final StartupVerification[] startupVerifications;

    DeviceLayerKind(
        StartupVerification... startupVerificationsRef
    )
    {
        startupVerifications = startupVerificationsRef;
    }

    public StartupVerification[] getStartupVerifications()
    {
        return startupVerifications;
    }
}
