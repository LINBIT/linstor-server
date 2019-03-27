package com.linbit.linstor.storage.kinds;

public enum StartupVerification
{
    UNAME,
    DRBD9, DRBD_PROXY,
    CRYPT_SETUP,
    LVM,
    ZFS
}
