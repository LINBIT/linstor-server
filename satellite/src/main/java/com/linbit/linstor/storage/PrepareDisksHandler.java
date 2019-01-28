package com.linbit.linstor.storage;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PrepareDisksHandler
{
    private static final String VG_PREFIX = "linstor_";

    private final CoreTimer timer;
    private final ErrorReporter errorReporter;

    private final ObjectMapper objectMapper;

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class NvmeDevice
    {
        public String DevicePath;
        public String Firmware;
        public Integer Index;
        public String ModelNumber;
        public String ProductName;
        public String SerialNumber;
        public Long UsedBytes;
        public Long MaximiumLBA;
        public Long PhysicalSize;
        public Long SectorSize;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class NvmeList
    {
        public List<NvmeDevice> Devices;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class PMEMDevice
    {
        public String dev;
        public String mode;
        public String map;
        //public Long size;  // ignore size as they have mixed datatypes int/string
        public String uuid;
        public Long sector_size;
        public String blockdev;
        public Long numa_node;
    }

    @Inject
    public PrepareDisksHandler(
        CoreTimer timerRef,
        ErrorReporter errorReporterRef
    )
    {
        timer = timerRef;
        errorReporter = errorReporterRef;

        objectMapper = new ObjectMapper();
    }

    private ApiCallRc.RcEntry pvCreate(final String devicePath)
        throws ChildProcessTimeoutException, IOException
    {
        final ExtCmd extCmd = new ExtCmd(timer, errorReporter);
        final ExtCmd.OutputData outputData = extCmd.exec("pvcreate", devicePath);
        ApiCallRc.RcEntry rcEntry;
        if (outputData.exitCode != 0)
        {
            String errStr = new String(outputData.stderrData);
            errorReporter.logError("Error creating PV: " + errStr);
            rcEntry = ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_ERROR | ApiConsts.MASK_CRT,
                errStr
            );
        }
        else
        {
            String infoStr = String.format("PV for device '%s' created.", devicePath);
            errorReporter.logInfo(infoStr);
            rcEntry = ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
                infoStr
            );
        }

        return rcEntry;
    }

    private Set<String> vgList()
        throws ChildProcessTimeoutException, IOException
    {
        Set<String> list;
        final ExtCmd extCmd = new ExtCmd(timer, errorReporter);
        final ExtCmd.OutputData outputData = extCmd.exec("vgs", "-o", "vg_name", "--noheadings");
        if (outputData.exitCode == 0)
        {
            String data = new String(outputData.stdoutData);
            list = Arrays.stream(data.split("\n"))
                .map(String::trim)
                .collect(Collectors.toSet());
        }
        else
        {
            throw new IOException(new String(outputData.stderrData));
        }

        return list;
    }

    private ApiCallRc.RcEntry vgCreate(final String vgName, final String devicePath)
        throws ChildProcessTimeoutException, IOException
    {
        final ExtCmd extCmd = new ExtCmd(timer, errorReporter);
        final ExtCmd.OutputData outputData = extCmd.exec("vgcreate", vgName, devicePath);
        ApiCallRc.RcEntry rcEntry;
        if (outputData.exitCode != 0)
        {
            String errStr = new String(outputData.stderrData);
            errorReporter.logError("Error creating VG: " + errStr);
            rcEntry = ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_ERROR | ApiConsts.MASK_CRT,
                errStr
            );
        }
        else
        {
            String infoStr = String.format("VG for device '%s' created.", devicePath);
            errorReporter.logInfo(infoStr);
            rcEntry = ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT, infoStr)
                .putObjRef("vg", vgName)
                .build();
        }
        return rcEntry;
    }

    private ApiCallRcImpl prepareNVME(final String nvmeFilter)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final ExtCmd.OutputData output = extCommand.exec("nvme", "list", "--output-format=json");
            final NvmeList nvmeList = objectMapper.readValue(output.getStdoutStream(), NvmeList.class);

            final Set<String> vgList = vgList();

            final List<NvmeDevice> filterDevices = nvmeList.Devices.stream()
                .filter(device -> device.ModelNumber.matches(nvmeFilter) &&
                    !vgList.contains(VG_PREFIX + device.SerialNumber))
                .collect(Collectors.toList());
            for (NvmeDevice device : filterDevices)
            {
                ApiCallRc.RcEntry rcEntry = pvCreate(device.DevicePath);
                apiCallRc.addEntry(rcEntry);
                if (!rcEntry.isError())
                {
                    final String vgName = VG_PREFIX + device.SerialNumber;
                    apiCallRc.addEntry(vgCreate(vgName, device.DevicePath));
                }
            }
        }
        catch (ChildProcessTimeoutException | IOException ex)
        {
            errorReporter.reportError(ex);
            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.MASK_ERROR, ex.getMessage()));
        }

        return apiCallRc;
    }

    private ApiCallRc.RcEntry setupPMEM(final String namespace)
        throws ChildProcessTimeoutException, IOException
    {
        final ExtCmd extCmd = new ExtCmd(timer, errorReporter);
        final ExtCmd.OutputData outputData = extCmd.exec(
            "ndctl",
            "create-namespace",
            "-f",
            "-e", namespace,
            "-m", "fsdax"
        );
        ApiCallRc.RcEntry rcEntry;
        if (outputData.exitCode != 0)
        {
            String errStr = new String(outputData.stderrData);
            errorReporter.logError("Error configuring PMEM: " + errStr);
            rcEntry = ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_ERROR | ApiConsts.MASK_CRT,
                errStr
            );
        }
        else
        {
            String infoStr = String.format("PMEM setup for namespace '%s'.", namespace);
            final PMEMDevice pmemDevice = objectMapper.readValue(outputData.getStdoutStream(), PMEMDevice.class);
            errorReporter.logInfo(infoStr);
            rcEntry = ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
                infoStr
            ).putObjRef("uuid", pmemDevice.uuid).build();
        }

        return rcEntry;
    }


    private ApiCallRc preparePMEM()
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final ExtCmd.OutputData output = extCommand.exec("ndctl", "list");
            final List<PMEMDevice> pmemList = Arrays.asList(objectMapper.readValue(
                output.getStdoutStream(),
                PMEMDevice[].class
            ));

            final Set<String> vgList = vgList();

            for (PMEMDevice device : pmemList)
            {
                String uuid = device.uuid;
                if (device.mode.equals("raw"))
                {
                    // "ndctl" "create-namespace" "-f" "-e" device.dev "-m" "fsdax"
                    final ApiCallRc.RcEntry apiCallRcEntry = setupPMEM(device.dev);
                    apiCallRc.addEntry(apiCallRcEntry);
                    if (!apiCallRcEntry.isError())
                    {
                        uuid = apiCallRcEntry.getObjRefs().get("uuid");
                    }
                }

                if (uuid != null && !vgList.contains(VG_PREFIX + uuid))
                {
                    final String devicePath = "/dev/" + device.blockdev;
                    ApiCallRc.RcEntry rcEntry = pvCreate(devicePath);
                    apiCallRc.addEntry(rcEntry);
                    if (!rcEntry.isError())
                    {
                        final String vgName = VG_PREFIX + uuid;
                        apiCallRc.addEntry(vgCreate(vgName, devicePath));
                    }
                }
            }
        }
        catch (ChildProcessTimeoutException | IOException ex)
        {
            errorReporter.reportError(ex);
            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.MASK_ERROR, ex.getMessage()));
        }

        if (apiCallRc.isEmpty())
        {
            apiCallRc.addEntry("All detected disks already prepared.", ApiConsts.MASK_SUCCESS);
        }

        return apiCallRc;
    }

    public ApiCallRc prepareDisks(final String nvmeFilter, final boolean detectPMEM)
    {
        ApiCallRcImpl apiCallRc = prepareNVME(nvmeFilter);

        if (detectPMEM)
        {
            apiCallRc.addEntries(preparePMEM());
        }

        return apiCallRc;
    }
}
