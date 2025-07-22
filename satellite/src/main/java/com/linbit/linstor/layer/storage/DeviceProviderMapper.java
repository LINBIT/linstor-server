package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.storage.diskless.DisklessProvider;
import com.linbit.linstor.layer.storage.ebs.EbsInitiatorProvider;
import com.linbit.linstor.layer.storage.ebs.EbsTargetProvider;
import com.linbit.linstor.layer.storage.file.FileProvider;
import com.linbit.linstor.layer.storage.file.FileThinProvider;
import com.linbit.linstor.layer.storage.lvm.LvmProvider;
import com.linbit.linstor.layer.storage.lvm.LvmThinProvider;
import com.linbit.linstor.layer.storage.spdk.SpdkLocalProvider;
import com.linbit.linstor.layer.storage.spdk.SpdkRemoteProvider;
import com.linbit.linstor.layer.storage.storagespaces.StorageSpacesProvider;
import com.linbit.linstor.layer.storage.storagespaces.StorageSpacesThinProvider;
import com.linbit.linstor.layer.storage.zfs.ZfsProvider;
import com.linbit.linstor.layer.storage.zfs.ZfsThinProvider;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.List;

@Singleton
public class DeviceProviderMapper
{
    private final LvmProvider lvmProvider;
    private final LvmThinProvider lvmThinProvider;
    private final ZfsProvider zfsProvider;
    private final ZfsThinProvider zfsThinProvider;
    private final DisklessProvider disklessProvider;
    private final FileProvider fileProvider;
    private final FileThinProvider fileThinProvider;
    private final SpdkLocalProvider spdkLocalProvider;
    private final SpdkRemoteProvider spdkRemoteProvider;
    private final EbsInitiatorProvider ebsInitProvider;
    private final EbsTargetProvider ebsTargetProvider;
    private final StorageSpacesProvider storageSpacesProvider;
    private final StorageSpacesThinProvider storageSpacesThinProvider;
    private final List<DeviceProvider> driverList;

    @Inject
    public DeviceProviderMapper(
        LvmProvider lvmProviderRef,
        LvmThinProvider lvmThinProviderRef,
        ZfsProvider zfsProviderRef,
        ZfsThinProvider zfsThinProviderRef,
        DisklessProvider disklessProviderRef,
        FileProvider fileProviderRef,
        FileThinProvider fileThinProviderRef,
        SpdkLocalProvider spdkLocalProviderRef,
        SpdkRemoteProvider spdkRemoteProviderRef,
        EbsInitiatorProvider ebsInitProviderRef,
        EbsTargetProvider ebsTargetProviderRef,
        StorageSpacesProvider storageSpacesProviderRef,
        StorageSpacesThinProvider storageSpacesThinProviderRef
    )
    {
        lvmProvider = lvmProviderRef;
        lvmThinProvider = lvmThinProviderRef;
        zfsProvider = zfsProviderRef;
        zfsThinProvider = zfsThinProviderRef;
        disklessProvider = disklessProviderRef;
        fileProvider = fileProviderRef;
        fileThinProvider = fileThinProviderRef;
        spdkLocalProvider = spdkLocalProviderRef;
        spdkRemoteProvider = spdkRemoteProviderRef;
        ebsInitProvider = ebsInitProviderRef;
        ebsTargetProvider = ebsTargetProviderRef;
        storageSpacesProvider = storageSpacesProviderRef;
        storageSpacesThinProvider = storageSpacesThinProviderRef;

        driverList = Arrays.asList(
            lvmProvider,
            lvmThinProvider,
            zfsProvider,
            zfsThinProvider,
            disklessProvider,
            fileProvider,
            fileThinProvider,
            spdkLocalProvider,
            spdkRemoteProvider,
            ebsInitProvider,
            ebsTargetProvider,
            storageSpacesProvider,
            storageSpacesThinProvider
        );
    }

    public List<DeviceProvider> getDriverList()
    {
        return driverList;
    }

    public DeviceProvider getDeviceProviderBy(StorPoolInfo storPool)
    {
        return getDeviceProviderByKind(storPool.getDeviceProviderKind());
    }

    public DeviceProvider getDeviceProviderByKind(DeviceProviderKind deviceProviderKind)
    {
        DeviceProvider devProvider;
        switch (deviceProviderKind)
        {
            case LVM:
                devProvider = lvmProvider;
                break;
            case LVM_THIN:
                devProvider = lvmThinProvider;
                break;
            case ZFS:
                devProvider = zfsProvider;
                break;
            case ZFS_THIN:
                devProvider = zfsThinProvider;
                break;
            case DISKLESS:
                devProvider = disklessProvider;
                break;
            case FILE:
                devProvider = fileProvider;
                break;
            case FILE_THIN:
                devProvider = fileThinProvider;
                break;
            case SPDK:
                devProvider = spdkLocalProvider;
                break;
            case REMOTE_SPDK:
                devProvider = spdkRemoteProvider;
                break;
            case EBS_INIT:
                devProvider = ebsInitProvider;
                break;
            case EBS_TARGET:
                devProvider = ebsTargetProvider;
                break;
            case STORAGE_SPACES:
                devProvider = storageSpacesProvider;
                break;
            case STORAGE_SPACES_THIN:
                devProvider = storageSpacesThinProvider;
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                throw new ImplementationError("A volume from a layer was asked for its provider type");
            default:
                throw new ImplementationError("Unknown storage provider found: " + deviceProviderKind);

        }
        return devProvider;
    }
}
