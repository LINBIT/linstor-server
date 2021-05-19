package com.linbit.linstor.storage.kinds;

import com.linbit.linstor.storage.DisklessDriverKind;
import com.linbit.linstor.storage.ExosDriverKind;
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
        true,
        false,
        true, // very thin :)
        true, // no disk, nothing to worry about
        new DisklessDriverKind() // compatibility - will be removed
    ),
    LVM(
        false,
        false,
        false,
        true,
        true,
        true,
        false,
        true,
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
        true,
        false,
        new LvmThinDriverKind(),
        ExtTools.LVM_THIN
    ),
    ZFS(
        true,
        true,
        true,
        true,
        true,
        true,
        false,
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
        true,
        true,
        false,
        new ZfsThinDriverKind(),
        ExtTools.ZFS
    ),
    FILE(
        true,
        false,
        false,
        true,
        true,
        true,
        false,
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
        true,
        false,
        new FileThinDriverKind(),
        ExtTools.LOSETUP
    ),
    SPDK(
        false,
        false,
        false,
        true,
        true,
        true,
        false,
        false,
        new SpdkDriverKind(),
        ExtTools.SPDK
    ),
    EXOS(
        false,
        false,
        false,
        true,
        false,
        true,
        false,
        true,
        new ExosDriverKind(),
        ExtTools.LSSCSI, ExtTools.SAS_PHY, ExtTools.SAS_DEVICE
    ),
    OPENFLEX_TARGET(
        false,
        false,
        false,
        false,
        true,

        true,
        false,
        true, // OpenFlex provides nvmeTarget, nothing to worry about
        new OpenflexTargetDriverKind()
    ),
    FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER(
        false,
        false,
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
    private final boolean isShrinkingSupported;
    private final boolean hasBackingDevice;
    private final boolean usesThinProvisioning;
    private final boolean isSharedVolumeSupported;
    @Deprecated
    private final StorageDriverKind storageDriverKind;
    private final ExtTools[] startupVerifications;

    DeviceProviderKind(
        boolean isSnapshotSupportedRef,
        boolean isSnapshotDependentRef,
        boolean isSnapshotShippingSupportedRef,
        boolean isResizeSupportedRef,
        boolean isShrinkingSupportedRef,
        boolean hasBackingDeviceRef,
        boolean usesThinProvisioningRef,
        boolean isSharedVolumeSupportedRef,
        StorageDriverKind storageDriverKindRef,
        ExtTools... startupVerificationsRef
    )
    {
        isSnapshotSupported = isSnapshotSupportedRef;
        isSnapshotDependent = isSnapshotDependentRef;
        isSnapshotShippingSupported = isSnapshotShippingSupportedRef;
        isResizeSupported = isResizeSupportedRef;
        isShrinkingSupported = isShrinkingSupportedRef;
        hasBackingDevice = hasBackingDeviceRef;
        usesThinProvisioning = usesThinProvisioningRef;
        isSharedVolumeSupported = isSharedVolumeSupportedRef;
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

    public boolean isSharedVolumeSupported()
    {
        return isSharedVolumeSupported;
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
                allowed = kind2.equals(LVM);
                break;
            case LVM_THIN:
                allowed = kind2.equals(LVM_THIN) || kind2.equals(ZFS) || kind2.equals(ZFS_THIN);
                break;
            case OPENFLEX_TARGET:
                allowed = kind2.equals(OPENFLEX_TARGET);
                break;
            case SPDK:
                allowed = kind2.equals(SPDK);
                break;
            case ZFS:
            case ZFS_THIN:
                allowed = kind2.equals(ZFS) || kind2.equals(ZFS_THIN) || kind2.equals(LVM_THIN);
                break;
            case EXOS:
                allowed = kind2.equals(EXOS);
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                break;
        }

        return allowed;
    }

    public boolean isShrinkingSupported()
    {
        return isShrinkingSupported;
    }
}
