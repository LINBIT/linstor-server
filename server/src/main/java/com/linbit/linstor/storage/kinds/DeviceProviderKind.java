package com.linbit.linstor.storage.kinds;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.DisklessDriverKind;
import com.linbit.linstor.storage.EbsInitiatorDriverKind;
import com.linbit.linstor.storage.EbsTargetDriverKind;
import com.linbit.linstor.storage.ExosDriverKind;
import com.linbit.linstor.storage.FileDriverKind;
import com.linbit.linstor.storage.FileThinDriverKind;
import com.linbit.linstor.storage.LvmDriverKind;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.RemoteSpdkDriverKind;
import com.linbit.linstor.storage.SpdkDriverKind;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageSpacesKind;
import com.linbit.linstor.storage.StorageSpacesThinKind;
import com.linbit.linstor.storage.ZfsDriverKind;
import com.linbit.linstor.storage.ZfsThinDriverKind;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

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
        true, // implicit
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
        true,
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
        true,
        new ZfsDriverKind(),
        ExtTools.ZFS_KMOD, ExtTools.ZFS_UTILS
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
        true,
        new ZfsThinDriverKind(),
        ExtTools.ZFS_KMOD, ExtTools.ZFS_UTILS
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
        false,  // might be technically possible but FILE is more for quick testing, so skip for now
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
        false,
        new FileThinDriverKind(),
        ExtTools.LOSETUP
    ),
    SPDK(
        true,
        true,
        false,
        true,
        true,
        true,
        false,
        false,
        false,
        new SpdkDriverKind(),
        ExtTools.SPDK
    ),
    REMOTE_SPDK(
        true,
        true,
        false,
        true,
        true,
        true,
        false,
        true,
        false,
        new RemoteSpdkDriverKind()
    ),
    @Deprecated(forRemoval = true)
    EXOS(
        false,
        false,
        false,
        true,
        false,
        true,
        false,
        true,
        false,
        new ExosDriverKind(),
        ExtTools.LSSCSI, ExtTools.SAS_PHY, ExtTools.SAS_DEVICE
    ),
    EBS_INIT(
        false,
        false,
        false,
        true,
        false,
        false,
        false,
        false,
        false,
        new EbsInitiatorDriverKind()
    ),
    EBS_TARGET(
        true,
        false,
        false,
        true,
        false,
        true,
        false,
        false,
        false,
        new EbsTargetDriverKind()
    ),
    STORAGE_SPACES( /* Microsoft storage spaces, Windows only */
        false,  /* snapshots currently not implemented */
        false,
        false,
        true,  /* resize yes (1GB granularity) */
        false, /* but no shrinking - TODO: possible via partition resize */

        true,   /* backing device yes */
        false,  /* no thin provisioning - TODO: this should be easy to implement */
        false,
        false,
        new StorageSpacesKind(),
        ExtTools.STORAGE_SPACES
    ),
    STORAGE_SPACES_THIN( /* Microsoft storage spaces, Windows only */
        false,  /* snapshots currently not implemented */
        false,
        false,
        true,  /* resize yes (1GB granularity) */
        false, /* but no shrinking - TODO: possible via partition resize */

        true,   /* backing device yes */
        true,  /* thin provisioning yes */
        false,
        false,
        new StorageSpacesThinKind(),
        ExtTools.STORAGE_SPACES
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
        false,
        null
    // no startup verifications, should always trigger an error anyways :)
    );

    public static final Version SP_MIXING_REQ_DRBD91_MIN_VERISON = new Version(9, 1, 18);
    public static final Version SP_MIXING_REQ_DRBD91_MAX_VERISON = new Version(9, 2, 0);
    public static final Version SP_MIXING_REQ_DRBD92_MIN_VERISON = new Version(9, 2, 7);

    private final boolean isSnapshotSupported;
    private final boolean isSnapshotDependent;
    private final boolean isSnapshotShippingSupported;
    private final boolean isResizeSupported;
    private final boolean isShrinkingSupported;
    private final boolean hasBackingDevice;
    private final boolean usesThinProvisioning;
    private final boolean isSharedVolumeSupported;
    private final boolean isCloneSupported;
    @Deprecated
    private final @Nullable StorageDriverKind storageDriverKind;
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
        boolean isCloneSupportedRef,
        @Nullable StorageDriverKind storageDriverKindRef,
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
        isCloneSupported = isCloneSupportedRef;
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

    public boolean isCloneSupported()
    {
        return isCloneSupported;
    }

    @Deprecated
    public @Nullable StorageDriverKind getStorageDriverKind()
    {
        return storageDriverKind;
    }

    public ExtTools[] getExtToolDependencies()
    {
        return startupVerifications;
    }

    public static boolean isMixingAllowed(
        DeviceProviderKind kind1,
        @Nullable Version drbdVersionNode1,
        DeviceProviderKind kind2,
        @Nullable Version drbdVersionNode2,
        boolean allowStorPoolMixing
    )
    {
        boolean allowed = false;
        boolean allowedWithRecentEnoughDrbdVersion = false;

        switch (kind1)
        {
            case DISKLESS:
                allowed = true;
                break;
            case FILE:
            case FILE_THIN:
                allowed = kind2.equals(FILE) || kind2.equals(FILE_THIN) || kind2.equals(DISKLESS);
                break;
            case LVM:
                allowed = kind2.equals(LVM) || kind2.equals(EXOS) ||
                    kind2.equals(STORAGE_SPACES) || kind2.equals(STORAGE_SPACES_THIN);
                allowedWithRecentEnoughDrbdVersion = kind2.equals(LVM_THIN) ||
                    kind2.equals(ZFS) || kind2.equals(ZFS_THIN);
                break;
            case LVM_THIN:
                allowed = kind2.equals(LVM_THIN) ||
                    kind2.equals(STORAGE_SPACES) || kind2.equals(STORAGE_SPACES_THIN);
                allowedWithRecentEnoughDrbdVersion = kind2.equals(LVM) ||
                    kind2.equals(ZFS) || kind2.equals(ZFS_THIN);
                break;
            case SPDK:
                allowed = kind2.equals(SPDK);
                break;
            case REMOTE_SPDK:
                allowed = kind2.equals(REMOTE_SPDK);
                break;
            case ZFS:
            case ZFS_THIN:
                allowed = kind2.equals(ZFS) || kind2.equals(ZFS_THIN) ||
                    kind2.equals(STORAGE_SPACES) || kind2.equals(STORAGE_SPACES_THIN);
                allowedWithRecentEnoughDrbdVersion = kind2.equals(LVM) || kind2.equals(LVM_THIN);
                break;
            case EXOS:
                allowed = kind2.equals(EXOS) || kind2.equals(LVM);
                break;
            case EBS_INIT: // fall-through
            case EBS_TARGET:
                allowed = kind2.equals(EBS_INIT) || kind2.equals(EBS_TARGET) || kind2.equals(DISKLESS);
                break;
            case STORAGE_SPACES:
            case STORAGE_SPACES_THIN:
                allowed = kind2.equals(STORAGE_SPACES) || kind2.equals(STORAGE_SPACES_THIN) ||
                    kind2.equals(LVM) || kind2.equals(LVM_THIN) ||
                    kind2.equals(ZFS) || kind2.equals(ZFS_THIN);
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                break;
        }

        if (!allowed && allowedWithRecentEnoughDrbdVersion && allowStorPoolMixing)
        {
            allowed = doesDrbdVersionSupportStorPoolMixing(drbdVersionNode1) &&
                doesDrbdVersionSupportStorPoolMixing(drbdVersionNode2);
        }

        return allowed;
    }

    public static boolean doesDrbdVersionSupportStorPoolMixing(@Nullable Version drbdVersion)
    {
        boolean supported = false;
        if (drbdVersion != null)
        {
            // check if DRBD version >= 9.1.18 but < 9.2.0
            supported = drbdVersion.greaterOrEqual(SP_MIXING_REQ_DRBD91_MIN_VERISON, false) &&
                !drbdVersion.greaterOrEqual(SP_MIXING_REQ_DRBD91_MAX_VERISON);
            // or >= 9.2.7
            supported |= drbdVersion.greaterOrEqual(SP_MIXING_REQ_DRBD92_MIN_VERISON);
        }
        return supported;
    }

    public boolean isShrinkingSupported()
    {
        return isShrinkingSupported;
    }
}
