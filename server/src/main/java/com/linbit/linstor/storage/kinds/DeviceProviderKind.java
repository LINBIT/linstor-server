package com.linbit.linstor.storage.kinds;

public enum DeviceProviderKind
{
    DRBD_DISKLESS(
        false,
        true,
        false,
        true, // very thin :)
        StartupVerifications.UNAME, StartupVerifications.DRBD9
    ),
    LVM(
        false,
        true,
        true,
        false,
        StartupVerifications.LVM
    ),
    LVM_THIN(
        true,
        true,
        true,
        true,
        StartupVerifications.LVM
    ),
    ZFS(
        true,
        true,
        true,
        false,
        StartupVerifications.ZFS
    ),
    ZFS_THIN(
        true,
        true,
        true,
        true,
        StartupVerifications.ZFS
    ),
    SWORDFISH_TARGET(
        false,
        false,
        true,
        false
    // no startup verifications
    ),
    SWORDFISH_INITIATOR(
        false,
        false,
        false,
        false
    // no startup verifications
    ),
    FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER(
        false,
        false,
        false,
        false
    // no startup verifications, should always trigger an error anyways :)
    );

    private final boolean isSnapshotSupported;
    private final boolean isResizeSupported;
    private final boolean hasBackingDevice;
    private final boolean usesThinProvisioning;
    private final StartupVerifications[] startupVerifications;

    DeviceProviderKind(
        boolean isSnapshotSupportedRef,
        boolean isResizeSupportedRef,
        boolean hasBackingDeviceRef,
        boolean usesThinProvisioningRef,
        StartupVerifications... startupVerificationsRef
    )
    {
        isSnapshotSupported = isSnapshotSupportedRef;
        isResizeSupported = isResizeSupportedRef;
        hasBackingDevice = hasBackingDeviceRef;
        usesThinProvisioning = usesThinProvisioningRef;
        startupVerifications = startupVerificationsRef;
    }

    public boolean isResizeSupported()
    {
        return isResizeSupported;
    }

    public boolean isSnapshotSupported()
    {
        return isSnapshotSupported;
    }

    public boolean hasBackingDevice()
    {
        return hasBackingDevice;
    }

    public boolean usesThinProvisioning()
    {
        return usesThinProvisioning;
    }

    public StartupVerifications[] getStartupVerifications()
    {
        return startupVerifications;
    }
}
