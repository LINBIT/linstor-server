package com.linbit.linstor.storage;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.linstor.storage.layer.provider.diskless.DisklessProvider;
import com.linbit.linstor.storage.layer.provider.file.FileProvider;
import com.linbit.linstor.storage.layer.provider.file.FileThinProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmThinProvider;
import com.linbit.linstor.storage.layer.provider.spdk.SpdkProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsThinProvider;

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
    private final SpdkProvider spdkProvider;
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
        SpdkProvider spdkProviderRef
    )
    {
        lvmProvider = lvmProviderRef;
        lvmThinProvider = lvmThinProviderRef;
        zfsProvider = zfsProviderRef;
        zfsThinProvider = zfsThinProviderRef;
        disklessProvider = disklessProviderRef;
        fileProvider = fileProviderRef;
        fileThinProvider = fileThinProviderRef;
        spdkProvider = spdkProviderRef;

        driverList = Arrays.asList(
            lvmProvider,
            lvmThinProvider,
            zfsProvider,
            zfsThinProvider,
            disklessProvider,
            fileProvider,
            fileThinProvider,
            spdkProvider
        );
    }

    public List<DeviceProvider> getDriverList()
    {
        return driverList;
    }

    public DeviceProvider getDeviceProviderByStorPool(StorPool storPool)
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
                devProvider = spdkProvider;
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                throw new ImplementationError("A volume from a layer was asked for its provider type");
            default:
                throw new ImplementationError("Unknown storage provider found: " + deviceProviderKind);

        }
        return devProvider;
    }
}
