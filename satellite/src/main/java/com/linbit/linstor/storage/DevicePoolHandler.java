package com.linbit.linstor.storage;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.RaidLevel;
import com.linbit.linstor.storage.layer.provider.utils.Commands;
import com.linbit.linstor.storage.utils.LvmCommands;
import com.linbit.linstor.storage.utils.ZfsCommands;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
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

    public String createVdoDevice(
        final ApiCallRcImpl apiCallRc,
        final String devicePath,
        final String poolName,
        long logicalSizeKib,
        long slabSizeKib
    )
    {
        String vdoDevicePath = null;
        try
        {
            List<String> cmd = new ArrayList<>();
            cmd.add("vdo");
            cmd.add("create");
            cmd.add("--name");
            cmd.add(poolName);
            cmd.add("--device");
            cmd.add(devicePath);
            if (logicalSizeKib > 0)
            {
                cmd.add("--vdoLogicalSize");
                cmd.add(logicalSizeKib + "K");
            }
            if (slabSizeKib > 0)
            {
                cmd.add("--vdoSlabSize");
                cmd.add(slabSizeKib + "K");
            }
            final String failMsg = "Unable to create VDO device: " + poolName;
            Commands.genericExecutor(
                extCmdFactory.create(),
                cmd.toArray(new String[0]),
                failMsg,
                failMsg
            );

            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                String.format("VDO '%s' on device '%s' created.", poolName, devicePath)));
            vdoDevicePath = "/dev/mapper/" + poolName;
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }
        return vdoDevicePath;
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
                apiCallRc.addEntries(createLVMPool(devicePaths, raidLevel, LvmThinDriverKind.VG_PREFIX + poolName));
                apiCallRc.addEntries(createLVMThinPool(LvmThinDriverKind.VG_PREFIX + poolName, poolName));
                break;
            case ZFS_THIN: // no differentiation between ZFS and ZFS_THIN pool. fall-through
            case ZFS:
                apiCallRc.addEntries(createZPool(devicePaths, raidLevel, poolName));
                break;

            case SPDK: // not implemented (yet) -> fall-through

                // the following cases make no sense, hence the fall-throughs
            case DISKLESS: // fall-through
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            case FILE: // fall-through
            case FILE_THIN: // fall-through
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
                LvmCommands.pvCreate(extCmdFactory.create(), devicePath);
                apiCallRc.addEntry(
                    ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                        String.format("PV for device '%s' created.", devicePath)
                    )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
                );
            }
            LvmCommands.vgCreate(extCmdFactory.create(), poolName, raidLevel, devicePaths);
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
            LvmCommands.vgRemove(extCmdFactory.create(), poolName);
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("VG with name '%s' removed.", poolName)
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
            );

            LvmCommands.pvRemove(extCmdFactory.create(), devicePaths);
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_DEL | ApiConsts.MASK_PHYSICAL_DEVICE,
                    String.format("PV for device(s) '%s' removed.", String.join(",", devicePaths))
                )
                    .putObjRef(ApiConsts.KEY_POOL_NAME, poolName)
                    .build()
            );
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
            LvmCommands.createThinPool(
                extCmdFactory.create(),
                lvmPoolName,
                thinPoolName
            );
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
            LvmCommands.delete(extCmdFactory.create(), lvmPoolName, thinPoolName);
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
                apiCallRc.addEntries(deleteLVMThinPool(devicePaths, LvmThinDriverKind.VG_PREFIX + poolName, poolName));
                break;
            case LVM:
                apiCallRc.addEntries(deleteLVMPool(devicePaths, poolName));
                break;
            case ZFS_THIN: // no differentiation between ZFS and ZFS_THIN pool. fall-through
            case ZFS:
                apiCallRc.addEntries(deleteZPool(devicePaths, poolName));
                break;

            // the following cases make no sense, hence the fall-throughs
            case DISKLESS: // fall-through
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            case FILE: // fall-through
            case FILE_THIN: // fall-through
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
}
