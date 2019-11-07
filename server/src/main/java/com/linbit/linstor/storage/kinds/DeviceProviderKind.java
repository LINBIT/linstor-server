package com.linbit.linstor.storage.kinds;

import com.linbit.linstor.storage.FileDriverKind;
import com.linbit.linstor.storage.FileThinDriverKind;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.DisklessDriverKind;
import com.linbit.linstor.storage.LvmDriverKind;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.SpdkDriverKind;
import com.linbit.linstor.storage.ZfsDriverKind;
import com.linbit.linstor.storage.ZfsThinDriverKind;
import com.linbit.linstor.storage.SwordfishInitiatorDriverKind;
import com.linbit.linstor.storage.SwordfishTargetDriverKind;

public enum DeviceProviderKind
{
    DISKLESS(
        false,
        false,
        true,
        false,
        true, // very thin :)
        new DisklessDriverKind() // compatibility - will be removed
    ),
    LVM(
        false,
        false,
        true,
        true,
        false,
        new LvmDriverKind(),
        ExtTools.LVM
    ),
    LVM_THIN(
        true,
        false,
        true,
        true,
        true,
        new LvmThinDriverKind(),
        ExtTools.LVM
    ),
    ZFS(
        true,
        true,
        true,
        true,
        false,
        new ZfsDriverKind(),
        ExtTools.ZFS
    ),
    ZFS_THIN(
        true,
        true,
        true,
        true,
        true,
        new ZfsThinDriverKind(),
        ExtTools.ZFS
    ),
    SWORDFISH_TARGET(
        false,
        false,
        false,
        true,
        false,
        new SwordfishTargetDriverKind()
    // no startup verifications
    ),
    SWORDFISH_INITIATOR(
        false,
        false,
        false,
        false,
        false,
        new SwordfishInitiatorDriverKind()
    // no startup verifications
    ),
    FILE(
        true,
        false,
        true,
        true,
        false,
        new FileDriverKind()
    ),
    FILE_THIN(
        true,
        false,
        true,
        true,
        true,
        new FileThinDriverKind()
    ),
    SPDK(
        false,
        false,
        true,
        true,
        false,
        new SpdkDriverKind(),
        ExtTools.SPDK
    ),
    FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER(
        false,
        false,
        false,
        false,
        false,
        null
    // no startup verifications, should always trigger an error anyways :)
    );

    private final boolean isSnapshotSupported;
    private final boolean isSnapshotDependent;
    private final boolean isResizeSupported;
    private final boolean hasBackingDevice;
    private final boolean usesThinProvisioning;
    @Deprecated
    private final StorageDriverKind storageDriverKind;
    private final ExtTools[] startupVerifications;

    DeviceProviderKind(
        boolean isSnapshotSupportedRef,
        boolean isSnapshotDependentRef,
        boolean isResizeSupportedRef,
        boolean hasBackingDeviceRef,
        boolean usesThinProvisioningRef,
        StorageDriverKind storageDriverKindRef,
        ExtTools... startupVerificationsRef
    )
    {
        isSnapshotSupported = isSnapshotSupportedRef;
        isSnapshotDependent = isSnapshotDependentRef;
        isResizeSupported = isResizeSupportedRef;
        hasBackingDevice = hasBackingDeviceRef;
        usesThinProvisioning = usesThinProvisioningRef;
        storageDriverKind = storageDriverKindRef;
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

    public boolean isSnapshotDependent()
    {
        return isSnapshotDependent;
    }

    public boolean hasBackingDevice()
    {
        return hasBackingDevice;
    }

    public boolean usesThinProvisioning()
    {
        return usesThinProvisioning;
    }

    @Deprecated
    public StorageDriverKind getStorageDriverKind()
    {
        return storageDriverKind;
    }

    public ExtTools[] getExtToolDependencies()
    {
        return startupVerifications;
    }
}
