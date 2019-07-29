package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.provider.utils.Commands;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class SysFsHandler
{
    private static final int BASE_HEX = 16;

    private static final String SYS_FS = "/sys/fs";
    private static final String CGROUP_BLKIO = SYS_FS + "/cgroup/blkio";

    private static final String THROTTLE_READ_BPS_DEVICE = CGROUP_BLKIO + "/blkio.throttle.read_bps_device";
    private static final String THROTTLE_WRITE_BPS_DEVICE = CGROUP_BLKIO + "/blkio.throttle.write_bps_device";

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ExtCmdFactory extCmdFactory;
    private final Map<String, String> cgroupBlkioThrottleReadBpsDeviceMap;
    private final Map<String, String> cgroupBlkioThrottleWriteBpsDeviceMap;
    private final Map<VlmProviderObject, String> deviceMajorMinorMap;
    private final Props satelliteProps;

    @Inject
    public SysFsHandler(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext apiCtxRef,
        ExtCmdFactory extCmdFactoryRef,
        @Named(LinStor.SATELLITE_PROPS) Props satellitePropsRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        extCmdFactory =  extCmdFactoryRef;
        satelliteProps = satellitePropsRef;
        cgroupBlkioThrottleReadBpsDeviceMap = new TreeMap<>();
        cgroupBlkioThrottleWriteBpsDeviceMap = new TreeMap<>();
        deviceMajorMinorMap = new HashMap<>();
    }

    public void updateSysFsSettings(Collection<Resource> rscs)
    {
        try
        {
            updateCgroupBlkioThrottleReadBpsDevice(rscs);
        }
        catch (AccessDeniedException | StorageException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void updateCgroupBlkioThrottleReadBpsDevice(Collection<Resource> rscsRef)
        throws AccessDeniedException, StorageException, InvalidKeyException
    {
        for (Resource rsc : rscsRef)
        {
            RscLayerObject rscLayerData = rsc.getLayerData(apiCtx);
            execForAllVlmData(
                rscLayerData,
                true,
                vlmData ->
                {
                    String majMin = getMajorMinor(vlmData);
                    if (majMin != null)
                    {
                        setThrottle(
                            vlmData,
                            majMin,
                            ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ,
                            cgroupBlkioThrottleReadBpsDeviceMap,
                            THROTTLE_READ_BPS_DEVICE
                        );
                        setThrottle(
                            vlmData,
                            majMin,
                            ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_WRITE,
                            cgroupBlkioThrottleWriteBpsDeviceMap,
                            THROTTLE_WRITE_BPS_DEVICE
                        );
                    }
                }
            );
        }
    }

    private void setThrottle(
        VlmProviderObject vlmDataRef,
        String majMinRef,
        String apiKey,
        Map<String, String> deviceThrottleMap,
        String sysFsPath
    )
        throws AccessDeniedException, InvalidKeyException, StorageException
    {
        Volume vlm = vlmDataRef.getVolume();
        Resource rsc = vlm.getResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        PriorityProps priorityProps = new PriorityProps();
        priorityProps.addProps(vlm.getProps(apiCtx));
        priorityProps.addProps(vlmDfn.getProps(apiCtx));
        priorityProps.addProps(rscGrp.getVolumeGroupProps(apiCtx, vlmDfn.getVolumeNumber()));
        priorityProps.addProps(rsc.getProps(apiCtx));
        priorityProps.addProps(rscDfn.getProps(apiCtx));
        priorityProps.addProps(rscGrp.getProps(apiCtx));

        for (StorPool storPool : LayerVlmUtils.getStorPoolSet(vlm, apiCtx))
        {
            priorityProps.addProps(storPool.getProps(apiCtx));
            priorityProps.addProps(storPool.getDefinition(apiCtx).getProps(apiCtx));
        }

        priorityProps.addProps(rsc.getAssignedNode().getProps(apiCtx));
        priorityProps.addProps(satelliteProps);

        String expectedThrottle = priorityProps.getProp(
            apiKey,
            ApiConsts.NAMESPC_SYS_FS
        );
        String knownThrottle = deviceThrottleMap.get(majMinRef);
        if (expectedThrottle != null &&
            (knownThrottle == null || !knownThrottle.equals(expectedThrottle)))
        {
            setSysFs(sysFsPath, majMinRef + " " + expectedThrottle);
            deviceThrottleMap.put(majMinRef, expectedThrottle);
        }
        else
        if (expectedThrottle == null && knownThrottle != null)
        {
            setSysFs(sysFsPath, majMinRef + " 0");
            deviceThrottleMap.remove(majMinRef);
        }
    }

    private String getMajorMinor(VlmProviderObject vlmDataRef) throws StorageException
    {
        String majMin = deviceMajorMinorMap.get(vlmDataRef);
        if (vlmDataRef.exists())
        {
            if (majMin == null)
            {
                String devicePath = vlmDataRef.getDevicePath();
                if (devicePath != null)
                {
                    OutputData outputData = Commands.genericExecutor(
                        extCmdFactory.create(),
                        new String[] {
                            "stat",
                            "-L", // follow links
                            "-c", "%t:%T",
                            devicePath
                        },
                        "Failed to find major:minor of device " + devicePath,
                        "Failed to find major:minor of device " + devicePath
                    );
                    majMin = new String(outputData.stdoutData).trim();
                    String[] split = majMin.split(":");
                    String major = Integer.toString(Integer.parseInt(split[0], BASE_HEX));
                    String minor = Long.toString(Long.parseLong(split[1], BASE_HEX));
                    majMin = major + ":" + minor;
                    deviceMajorMinorMap.put(vlmDataRef, majMin);
                }
            }
        }
        else
        {
            deviceMajorMinorMap.remove(vlmDataRef);
        }

        return majMin;
    }

    private void execForAllVlmData(
        RscLayerObject rscLayerData,
        boolean recursive,
        Executor<VlmProviderObject> consumer
    )
        throws StorageException, AccessDeniedException, InvalidKeyException
    {
        for (VlmProviderObject vlmData : rscLayerData.getVlmLayerObjects().values())
        {
            consumer.exec(vlmData);
        }
        if (recursive)
        {
            for (RscLayerObject childRscData : rscLayerData.getChildren())
            {
                execForAllVlmData(childRscData, recursive, consumer);
            }
        }
    }

    private void setSysFs(String path, String data) throws StorageException
    {
        try
        {
            Files.write(Paths.get(path), data.getBytes());
            errorReporter.logTrace("SysFs: '%s' > %s", data, path);
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to write '" + data + "' to path " + path);
        }
    }


    private interface Executor<T>
    {
        void exec(T obj) throws StorageException, AccessDeniedException, InvalidKeyException;
    }
}
