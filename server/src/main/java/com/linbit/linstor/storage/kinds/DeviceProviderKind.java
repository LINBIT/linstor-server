package com.linbit.linstor.storage.kinds;

import com.linbit.linstor.storage.DisklessDriverKind;
import com.linbit.linstor.storage.FileDriverKind;
import com.linbit.linstor.storage.FileThinDriverKind;
import com.linbit.linstor.storage.LvmDriverKind;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.OpenflexTargetDriverKind;
import com.linbit.linstor.storage.SpdkDriverKind;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.ZfsDriverKind;
import com.linbit.linstor.storage.ZfsThinDriverKind;

import javax.annotation.Nonnull;

public enum DeviceProviderKind
{
    DISKLESS(
        false,
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
        true,
        new LvmThinDriverKind(),
        ExtTools.LVM_THIN
    ),
    ZFS(
        true,
        true,
        false,
        true,
        true,
        false,
        new ZfsDriverKind(),
        ExtTools.ZFS
    ),
    ZFS_THIN(
        true,
        true,
        false,
        true,
        true,
        true,
        new ZfsThinDriverKind(),
        ExtTools.ZFS
    ),
    FILE(
        true,
        false,
        false,
        true,
        true,
        false,
        new FileDriverKind(),
        ExtTools.LOSETUP
    ),
    FILE_THIN(
        true,
        false,
        false,
        true,
        true,
        true,
        new FileThinDriverKind(),
        ExtTools.LOSETUP
    ),
    SPDK(
        false,
        false,
        false,
        true,
        true,
        false,
        new SpdkDriverKind(),
        ExtTools.SPDK
    ),
    OPENFLEX_TARGET(
        false,
        false,
        false,
        false,
        true,
        false,
        new OpenflexTargetDriverKind()
    ),
    FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER(
        false,
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
    private final boolean isSnapshotShippingSupported;
    private final boolean isResizeSupported;
    private final boolean hasBackingDevice;
    private final boolean usesThinProvisioning;
    @Deprecated
    private final StorageDriverKind storageDriverKind;
    private final ExtTools[] startupVerifications;

    DeviceProviderKind(
        boolean isSnapshotSupportedRef,
        boolean isSnapshotDependentRef,
        boolean isSnapshotShippingSupportedRef,
        boolean isResizeSupportedRef,
        boolean hasBackingDeviceRef,
        boolean usesThinProvisioningRef,
        StorageDriverKind storageDriverKindRef,
        ExtTools... startupVerificationsRef
    )
    {
        isSnapshotSupported = isSnapshotSupportedRef;
        isSnapshotDependent = isSnapshotDependentRef;
        isSnapshotShippingSupported = isSnapshotShippingSupportedRef;
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

    public boolean isSnapshotShippingSupported()
    {
        return isSnapshotShippingSupported;
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

    public static boolean isMixingAllowed(@Nonnull DeviceProviderKind kind1, @Nonnull DeviceProviderKind kind2)
    {
        boolean allowed = false;
        switch (kind1)
        {
            case DISKLESS:
                allowed = true;
                break;
            case FILE:
            case FILE_THIN:
                allowed = kind2.equals(FILE) || kind2.equals(FILE_THIN);
                break;
            case LVM:
            case LVM_THIN:
                allowed = kind2.equals(LVM) || kind2.equals(LVM_THIN);
                break;
            case OPENFLEX_TARGET:
                allowed = kind2.equals(OPENFLEX_TARGET);
                break;
            case SPDK:
                allowed = kind2.equals(SPDK);
                break;
            case ZFS:
            case ZFS_THIN:
                allowed = kind2.equals(ZFS) || kind2.equals(ZFS_THIN);
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                break;
        }

        return allowed;
    }
}
