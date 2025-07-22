package com.linbit.linstor.layer.storage;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LvmVolumeType;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkLocalCommands;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.RaidLevel;
import com.linbit.linstor.storage.utils.Commands;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Singleton
public class DevicePoolHandler
{
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public DevicePoolHandler(ErrorReporter errorReporterRef, ExtCmdFactory extCmdFactoryRef)
    {
        this.errorReporter = errorReporterRef;
        this.extCmdFactory = extCmdFactoryRef;
    }

    public @Nullable String createVdoDevice(
        final ApiCallRcImpl apiCallRc,
        final DeviceProviderKind kind,
        final String vgName,
        final String poolName,
        long logicalSizeKib,
        long slabSizeKib
    )
    {
        String vdoPool = null;
        try
        {
            List<String> cmd = new ArrayList<>();
            cmd.add("lvcreate");
            cmd.add("-y");
            cmd.add("--type");
            cmd.add("vdo");
            cmd.add("--name");
            cmd.add(poolName);
            cmd.add("--extents");
            // need some space for lvm-thin metadata
            cmd.add((kind == DeviceProviderKind.LVM_THIN ? "98" : "100") + "%FREE");
            if (logicalSizeKib > 0)
            {
                cmd.add("--virtualsize");
                cmd.add(logicalSizeKib + "K");
            }
            if (slabSizeKib > 0)
            {
                cmd.add("--vdosettings=slab_size_mb=" + (slabSizeKib / 1024));
            }
            cmd.add(vgName + "/vdopool0");

            {
                final String failMsg = "Unable to create VDO device: " + vgName + "/" + poolName;
                Commands.genericExecutor(
                    extCmdFactory.create(),
                    cmd.toArray(new String[0]),
                    failMsg,
                    failMsg
                );
            }

            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                String.format("VDO '%s' on VG '%s' created.", poolName, vgName)));

            if (kind == DeviceProviderKind.LVM_THIN)
            {
                final String failMsg = "Unable to convert VDO device to thin: " + vgName + "/" + poolName;
                Commands.genericExecutor(
                    extCmdFactory.create(),
                    new String[]{"lvconvert", "-y", "--type", "thin-pool", vgName + "/" + poolName},
                    failMsg,
                    failMsg
                );
            }

            vdoPool = vgName + "/" + poolName;
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }
        return vdoPool;
    }

    public ApiCallRc createDevicePool(
        final DeviceProviderKind deviceProviderKind,
        final List<String> devicePaths,
        final RaidLevel raidLevel,
        final String poolName
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        switch (deviceProviderKind)
        {
            case LVM:
                apiCallRc.addEntries(createLVMPool(devicePaths, raidLevel, poolName));
                break;
            case LVM_THIN:
                apiCallRc.addEntries(createLVMPool(devicePaths, raidLevel, LvmThinDriverKind.VGName(poolName)));
                apiCallRc.addEntries(
                    createLVMThinPool(LvmThinDriverKind.VGName(poolName), LvmThinDriverKind.LVName(poolName)));
                break;
            case ZFS_THIN: // no differentiation between ZFS and ZFS_THIN pool. fall-through
            case ZFS:
                apiCallRc.addEntries(createZPool(devicePaths, raidLevel, poolName));
                break;
            case SPDK:
                apiCallRc.addEntries(createSpdkLocalPool(devicePaths, poolName));
                break;

            case REMOTE_SPDK: // for now, fall-through, might change in future
            case EBS_INIT: // for now, fall-through, might change in future
            case EBS_TARGET: // for now, fall-through, might change in future

            // the following cases make no sense, hence the fall-throughs
            case DISKLESS: // fall-through
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            case FILE: // fall-through
            case FILE_THIN: // fall-through
            case STORAGE_SPACES: // fall-through
            case STORAGE_SPACES_THIN: // fall-through
            default:
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_PROVIDER,
                        "Create device pool not supported for provider: " + deviceProviderKind
                    )
                );
                break;
        }

        return apiCallRc;
    }

    private ApiCallRc createLVMPool(final List<String> devicePaths, final RaidLevel raidLevel, final String poolName)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            for (final String devicePath : devicePaths)
            {
                LvmCommands.pvCreate(
                    extCmdFactory.create(),
                    devicePath,
                    LvmUtils.getLvmFilterByPhysicalVolumes(devicePath)
                );
                apiCallRc.addEntry(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                        String.format("PV for device '%s' created.", devicePath)
                    )
                        .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                        .build()
                );
            }
            LvmCommands.vgCreate(
                extCmdFactory.create(),
                poolName,
                raidLevel,
                devicePaths,
                LvmUtils.getLvmFilterByPhysicalVolumes(devicePaths)
            );
            LvmUtils.recacheNext();

            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("VG for devices [%s] with name '%s' created.",
                        String.join(", ", devicePaths),
                        poolName)
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
            );
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }

        return apiCallRc;
    }

    private ApiCallRc deleteLVMPool(final List<String> devicePaths, final String poolName)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(poolName),
                filter -> LvmCommands.vgRemove(
                    extCmdFactory.create(),
                    poolName,
                    filter
                )
            );
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("VG with name '%s' removed.", poolName)
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
            );

            LvmCommands.pvRemove(
                extCmdFactory.create(),
                devicePaths,
                LvmUtils.getLvmFilterByPhysicalVolumes(devicePaths)
            );
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("PV for device(s) '%s' removed.", String.join(",", devicePaths))
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
            );
            LvmUtils.recacheNext();
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_STOR_POOL_CONFIGURATION_ERROR, storExc));
        }

        return apiCallRc;
    }

    private ApiCallRc createLVMThinPool(
        final String lvmPoolName,
        final String thinPoolName
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(lvmPoolName),
                config -> LvmCommands.createThinPool(
                    extCmdFactory.create(),
                    lvmPoolName,
                    thinPoolName, config
                )
            );
            LvmUtils.recacheNext();

            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("Thin-pool '%s' in LVM-pool '%s' created.", thinPoolName, lvmPoolName)
                ).putObjRef(ApiConsts.KEY_POOL_NAME, lvmPoolName + "/" + thinPoolName).build()
            );
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }
        return apiCallRc;
    }

    private ApiCallRc deleteLVMThinPool(
        final List<String> devicePaths,
        final String lvmPoolName,
        final String thinPoolName
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(lvmPoolName),
                config -> LvmCommands.delete(
                    extCmdFactory.create(),
                    lvmPoolName,
                    thinPoolName,
                    config,
                    LvmVolumeType.THIN_POOL
                )
            );
            LvmUtils.recacheNext();

            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("Thin lv with name '%s' removed.", thinPoolName)
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, lvmPoolName + "/" + thinPoolName)
                    .build()
            );

            apiCallRc.addEntries(deleteLVMPool(devicePaths, lvmPoolName));
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_STOR_POOL_CONFIGURATION_ERROR, storExc));
        }

        return apiCallRc;
    }

    private ApiCallRc createZPool(
        final List<String> devicePaths,
        final RaidLevel raidLevel,
        final String zPoolName
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try
        {
            ZfsCommands.createZPool(
                extCmdFactory.create(),
                devicePaths,
                raidLevel,
                zPoolName
            );
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format(
                        "ZPool '%s' on device(s) [%s] created.",
                        zPoolName,
                        String.join(", ", devicePaths))
                ).putObjRef(ApiConsts.KEY_POOL_NAME, zPoolName).build()
            );
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }

        return apiCallRc;
    }

    private ApiCallRc deleteZPool(final List<String> devicePaths, final String zPoolName)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try
        {
            ZfsCommands.deleteZPool(
                extCmdFactory.create(),
                zPoolName
            );
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format(
                        "ZPool '%s' deleted.",
                        zPoolName)
                ).putObjRef(ApiConsts.KEY_POOL_NAME, zPoolName).build()
            );

            Commands.wipeFs(extCmdFactory.create(), devicePaths);
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }
        return apiCallRc;
    }

    private ApiCallRc createSpdkLocalPool(final List<String> pciAddresses, final String poolName)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            String lvolStoreName;
            List<String> nvmeBdevs = new ArrayList<>();
            for (final String pciAddress : pciAddresses)
            {
                final String bdevName = new String(
                    SpdkLocalCommands.nvmeBdevCreate(
                        extCmdFactory.create(),
                        pciAddress
                    ).stdoutData
                ).trim();
                nvmeBdevs.add(bdevName);
                apiCallRc.addEntry(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                        String.format("Nvme bdev for device '%s' created.", pciAddress)
                    )
                        .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                        .build()
                );
            }

            if (nvmeBdevs.size() > 1)
            {
                SpdkLocalCommands.nvmeRaidBdevCreate(extCmdFactory.create(), poolName, nvmeBdevs);
                lvolStoreName = poolName;
            }
            else
            {
                lvolStoreName = nvmeBdevs.get(0);
            }

            SpdkLocalCommands.lvolStoreCreate(extCmdFactory.create(), lvolStoreName, poolName);
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("Lvol store for devices [%s] with name '%s' created.",
                        String.join(", ", pciAddresses),
                        poolName)
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
            );

        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }

        return apiCallRc;
    }

    private ApiCallRc deleteSPDKPool(final List<String> pciAddresses, final String poolName)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            SpdkLocalCommands.lvolStoreRemove(extCmdFactory.create(), poolName);
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("Lvol store with name '%s' removed.", poolName)
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
            );

            if (new String(SpdkLocalCommands.listRaidBdevsAll(
                extCmdFactory.create()).stdoutData).trim().matches("(.*)\\b" + poolName + "\\b(.*)"))
            {
                SpdkLocalCommands.nvmeRaidBdevRemove(extCmdFactory.create(), poolName);
                apiCallRc.addEntry(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                        String.format("RAID bdev with name '%s' removed.", poolName)
                    )
                        .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                        .build()
                );
            }

            for (final String pciAddress : pciAddresses)
            {
                SpdkLocalCommands.nvmeBdevRemove(extCmdFactory.create(), pciAddress);
                apiCallRc.addEntry(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                        String.format("Nvme bdev for device '%s' removed.", pciAddress)
                    )
                        .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                        .build()
                );
            }
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_STOR_POOL_CONFIGURATION_ERROR, storExc));
        }

        return apiCallRc;
    }

    public ApiCallRc deleteDevicePool(
        final DeviceProviderKind deviceProviderKind,
        final List<String> devicePaths,
        final String poolName
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        switch (deviceProviderKind)
        {
            case LVM_THIN:
                apiCallRc.addEntries(deleteLVMThinPool(
                    devicePaths, LvmThinDriverKind.VGName(poolName), LvmThinDriverKind.LVName(poolName)));
                break;
            case LVM:
                apiCallRc.addEntries(deleteLVMPool(devicePaths, poolName));
                break;
            case ZFS_THIN: // no differentiation between ZFS and ZFS_THIN pool. fall-through
            case ZFS:
                apiCallRc.addEntries(deleteZPool(devicePaths, poolName));
                break;
            case SPDK:
                apiCallRc.addEntries(deleteSPDKPool(devicePaths, poolName));
                break;

            case REMOTE_SPDK: // for now, fall-through, might change in future
            case EBS_INIT: // for now, fall-through, might change in future
            case EBS_TARGET: // for now, fall-through, might change in future

            // the following cases make no sense, hence the fall-throughs
            case DISKLESS: // fall-through
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            case FILE: // fall-through
            case FILE_THIN: // fall-through
            case STORAGE_SPACES: // fall-through
            case STORAGE_SPACES_THIN: // fall-through
            default:
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_PROVIDER,
                        "Delete device pool not supported for provider: " + deviceProviderKind
                    )
                );
                break;
        }

        return apiCallRc;
    }

    public ApiCallRc checkPoolExists(
        final DeviceProviderKind deviceProviderKind,
        final String poolName
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            switch (deviceProviderKind)
            {
                case LVM_THIN: // fall-through
                case LVM:
                    if (LvmUtils.checkVgExistsBool(extCmdFactory, poolName))
                    {
                        apiCallRc.addEntry("Volume group name already used.", ApiConsts.FAIL_EXISTS_STOR_POOL);
                    }
                    break;
                case ZFS_THIN: // no differentiation between ZFS and ZFS_THIN pool. fall-through
                case ZFS:
                    if (ZfsUtils.getZPoolList(extCmdFactory.create()).contains(poolName))
                    {
                        apiCallRc.addEntry("Zpool name already used.", ApiConsts.FAIL_EXISTS_STOR_POOL);
                    }
                    break;

                case REMOTE_SPDK: // for now, fall-through, might change in future
                case EBS_INIT: // for now, fall-through, might change in future
                case EBS_TARGET: // for now, fall-through, might change in future

                case SPDK: // fall-through for now
                // the following cases make no sense, hence the fall-throughs
                case DISKLESS: // fall-through
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
                case FILE: // fall-through
                case FILE_THIN: // fall-through
                case STORAGE_SPACES: // fall-through
                case STORAGE_SPACES_THIN: // fall-through
                default:
                    break;
            }
        }
        catch (StorageException storExc)
        {
            apiCallRc.addEntry("Unable to check storage pools", ApiConsts.FAIL_STOR_POOL_CONFIGURATION_ERROR);
        }
        return apiCallRc;
    }
}
