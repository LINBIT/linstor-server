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
    private static final String VG_PREFIX = "linstor_";

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
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
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
                apiCallRc.addEntries(createLVMPool(devicePaths, raidLevel, VG_PREFIX + poolName));
                apiCallRc.addEntries(createLVMThinPool(VG_PREFIX + poolName, poolName));
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
                apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                    ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
                    String.format("PV for device '%s' created.", devicePath))
                );
            }
            LvmCommands.vgCreate(extCmdFactory.create(), poolName, raidLevel, devicePaths);
            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
                String.format("VG for devices [%s] with name '%s' created.",
                    String.join(", ", devicePaths),
                    poolName)
                )
            );
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
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
            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
                String.format("Thin-pool '%s' in LVM-pool '%s' created.", thinPoolName, lvmPoolName)));
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
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
            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
                String.format(
                    "ZPool '%s' on device(s) [%s] created.",
                    zPoolName,
                    String.join(", ", devicePaths)))
            );
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }

        return apiCallRc;
    }
}
