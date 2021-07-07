package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.storage.diskless.DisklessProvider;
import com.linbit.linstor.layer.storage.exos.ExosProvider;
import com.linbit.linstor.layer.storage.file.FileProvider;
import com.linbit.linstor.layer.storage.file.FileThinProvider;
import com.linbit.linstor.layer.storage.lvm.LvmProvider;
import com.linbit.linstor.layer.storage.lvm.LvmThinProvider;
import com.linbit.linstor.layer.storage.spdk.SpdkLocalProvider;
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
    private final ExosProvider exosProvider;
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
        ExosProvider exosProviderRef
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
        exosProvider = exosProviderRef;

        driverList = Arrays.asList(
            lvmProvider,
            lvmThinProvider,
            zfsProvider,
            zfsThinProvider,
            disklessProvider,
            fileProvider,
            fileThinProvider,
            spdkLocalProvider,
            exosProvider
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
                devProvider = spdkLocalProvider;
                break;
            case EXOS:
                devProvider = exosProvider;
                break;
            case OPENFLEX_TARGET:
                throw new ImplementationError("Openflex does not have a deviceProvider, but is a layer instead");
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                throw new ImplementationError("A volume from a layer was asked for its provider type");
            default:
                throw new ImplementationError("Unknown storage provider found: " + deviceProviderKind);

        }
        return devProvider;
    }
}
