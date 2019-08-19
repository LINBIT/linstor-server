package com.linbit.linstor.storage.kinds;

public enum StartupVerification
{
    DRBD9, DRBD_PROXY,
    CRYPT_SETUP,
    LVM,
    ZFS,
    NVME
}
