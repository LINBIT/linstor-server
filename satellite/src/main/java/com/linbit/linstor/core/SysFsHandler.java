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
import com.linbit.linstor.layer.storage.utils.Commands;
import com.linbit.linstor.layer.storage.utils.Commands.RetryHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.ExceptionThrowingBiFunction;

import static com.linbit.linstor.layer.storage.spdk.utils.SpdkCommands.SPDK_RPC_SCRIPT;
import static com.linbit.linstor.layer.storage.spdk.utils.SpdkUtils.SPDK_PATH_PREFIX;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class SysFsHandler
{
    private static final int BASE_HEX = 16;

    private static final String SYS_FS = "/sys/fs";
    private static final String CGROUP_BLKIO = SYS_FS + "/cgroup/blkio";

    private static final String THROTTLE_READ_BPS_DEVICE = CGROUP_BLKIO + "/blkio.throttle.read_bps_device";
    private static final String THROTTLE_WRITE_BPS_DEVICE = CGROUP_BLKIO + "/blkio.throttle.write_bps_device";
    private static final String THROTTLE_READ_IOPS_DEVICE = CGROUP_BLKIO + "/blkio.throttle.read_iops_device";
    private static final String THROTTLE_WRITE_IOPS_DEVICE = CGROUP_BLKIO + "/blkio.throttle.write_iops_device";

    private static final String SPDK_THROTTLE_READ_MBPS = "--r_mbytes_per_sec";
    private static final String SPDK_THROTTLE_WRITE_MBPS = "--w_mbytes_per_sec";

    private static final String SYS_FS_BCACHE = SYS_FS + "/bcache";
    private static final Path PATH_SYS_FS_BCACHE = Paths.get(SYS_FS_BCACHE);
    private static final Pattern PATTERN_PATH_SYS_FS_BCACHE_BDEV = Pattern.compile(
        "^" + SYS_FS_BCACHE + "/([^/]+)/bdev[^/]+"
    );

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ExtCmdFactory extCmdFactory;
    private final Map<String, String> cgroupBlkioThrottleReadBpsDeviceMap;
    private final Map<String, String> cgroupBlkioThrottleWriteBpsDeviceMap;
    private final Map<String, String> cgroupBlkioThrottleReadIopsDeviceMap;
    private final Map<String, String> cgroupBlkioThrottleWriteIopsDeviceMap;
    private final Map<VlmProviderObject<Resource>, String> deviceMajorMinorMap;
    private final Props satelliteProps;

    public static final String DEVNAME = "DEVNAME";
    public static final String DEVTYPE = "DEVTYPE";
    public static final String DEVTYPE_DISK = "disk";
    public static final String DEVTYPE_PARTITION = "partition";

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
        cgroupBlkioThrottleReadIopsDeviceMap = new TreeMap<>();
        cgroupBlkioThrottleWriteIopsDeviceMap = new TreeMap<>();
        deviceMajorMinorMap = new HashMap<>();
    }

    public void updateSysFsSettings(Collection<Resource> updateList, Collection<Resource> deleteList)
    {
        try
        {
            updateCgroupBlkioThrottleReadBpsDevice(updateList);
            updateCgroupBlkioThrottleReadIopsDevice(updateList);
            cleanupCaches(deleteList);
        }
        catch (AccessDeniedException | StorageException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void cleanupCaches(Collection<Resource> deleteListRef)
        throws AccessDeniedException, InvalidKeyException, StorageException
    {
        for (Resource rsc : deleteListRef)
        {
            AbsRscLayerObject<Resource> rscLayerData = rsc.getLayerData(apiCtx);
            execForAllVlmData(
                rscLayerData,
                true,
                vlmData ->
                {
                    String identifier;
                    if (vlmData instanceof SpdkData)
                    {
                        identifier = vlmData.getDevicePath();
                    }
                    else
                    {
                        identifier = getMajorMinor(vlmData);
                    }

                    if (identifier != null)
                    {
                        // cleaning up throttle caches
                        cgroupBlkioThrottleReadBpsDeviceMap.remove(identifier);
                        cgroupBlkioThrottleWriteBpsDeviceMap.remove(identifier);

                        cgroupBlkioThrottleReadIopsDeviceMap.remove(identifier);
                        cgroupBlkioThrottleWriteIopsDeviceMap.remove(identifier);

                        deviceMajorMinorMap.remove(vlmData);
                    }
                }
            );
        }
    }

    private void updateCgroupBlkioThrottleReadBpsDevice(Collection<Resource> updateList)
        throws AccessDeniedException, StorageException, InvalidKeyException
    {
        execForAllLowestLocalVlmData(updateList, (vlmData, identifier) ->
        {
            setThrottle(
                vlmData,
                identifier,
                ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ,
                cgroupBlkioThrottleReadBpsDeviceMap,
                THROTTLE_READ_BPS_DEVICE
            );
            setThrottle(
                vlmData,
                identifier,
                ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_WRITE,
                cgroupBlkioThrottleWriteBpsDeviceMap,
                THROTTLE_WRITE_BPS_DEVICE
            );
        });
    }

    private void updateCgroupBlkioThrottleReadIopsDevice(Collection<Resource> updateList)
        throws AccessDeniedException, StorageException, InvalidKeyException
    {
        execForAllLowestLocalVlmData(updateList, (vlmData, identifier) ->
        {
            setThrottle(
                vlmData,
                identifier,
                ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ_IOPS,
                cgroupBlkioThrottleReadIopsDeviceMap,
                THROTTLE_READ_IOPS_DEVICE
            );
            setThrottle(
                vlmData,
                identifier,
                ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_WRITE_IOPS,
                cgroupBlkioThrottleWriteIopsDeviceMap,
                THROTTLE_WRITE_IOPS_DEVICE
            );
        });
    }

    private void execForAllLowestLocalVlmData(
        Collection<Resource> updateList,
        BiExecutor<VlmProviderObject<Resource>, String> consumer
    )
        throws AccessDeniedException, InvalidKeyException, StorageException
    {
        for (Resource rsc : updateList)
        {
            AbsRscLayerObject<Resource> rscLayerData = rsc.getLayerData(apiCtx);
            execForAllVlmData(
                rscLayerData,
                true,
                vlmData ->
                {
                    AbsRscLayerObject<Resource> rscLayerObject = vlmData.getRscLayerObject();
                    if (rscLayerObject.getResourceNameSuffix().equals(RscLayerSuffixes.SUFFIX_DATA))
                    {
                        boolean isLowestLocalDevices = true;
                        if (!vlmData.getRscLayerObject().getChildren().isEmpty())
                        {
                            VlmProviderObject<Resource> childVlmData = vlmData.getRscLayerObject()
                                .getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA).getVlmProviderObject(vlmData.getVlmNr());
                            if (
                                childVlmData != null &&
                                    childVlmData.getDevicePath() != null &&
                                    !childVlmData.getDevicePath().trim().isEmpty()
                            )
                            {
                                isLowestLocalDevices = false;
                            }
                        }
                        String identifier;
                        if (vlmData instanceof SpdkData)
                        {
                            identifier = vlmData.getDevicePath();
                        }
                        else
                        {
                            identifier = getMajorMinor(vlmData);
                        }

                        if (identifier != null && isLowestLocalDevices)
                        {
                            consumer.exec(vlmData, identifier);
                        }
                    }
                }
            );
        }
    }

    private void setThrottle(
        VlmProviderObject<Resource> vlmDataRef,
        String identifier,
        String apiKey,
        Map<String, String> deviceThrottleMap,
        String sysFsPath
    )
        throws AccessDeniedException, InvalidKeyException, StorageException
    {
        Volume vlm = (Volume) vlmDataRef.getVolume();
        Resource rsc = vlm.getAbsResource();
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

        for (StorPool storPool : LayerVlmUtils.getStorPoolSet(vlm, apiCtx, true))
        {
            priorityProps.addProps(storPool.getProps(apiCtx));
            priorityProps.addProps(storPool.getDefinition(apiCtx).getProps(apiCtx));
        }

        priorityProps.addProps(rsc.getNode().getProps(apiCtx));
        priorityProps.addProps(satelliteProps);

        String expectedThrottle = priorityProps.getProp(
            apiKey,
            ApiConsts.NAMESPC_SYS_FS
        );
        String knownThrottle = deviceThrottleMap.get(identifier);

        if (expectedThrottle == null && knownThrottle != null)
        {
            if (vlmDataRef instanceof SpdkData)
            {
                setSpdkIO(identifier, apiKey, "0");
            }
            else
            {
                setSysFs(sysFsPath, identifier + " 0");
            }
            deviceThrottleMap.remove(identifier);
        }
        else
        if (
            expectedThrottle != null &&
            (knownThrottle == null || !knownThrottle.equals(expectedThrottle))
        )
        {
            if (vlmDataRef instanceof SpdkData)
            {
                setSpdkIO(identifier, apiKey, expectedThrottle);
            }
            else
            {
                setSysFs(sysFsPath, identifier + " " + expectedThrottle);
            }
            deviceThrottleMap.put(identifier, expectedThrottle);
        }
        else
        {
            errorReporter.logTrace(
                "SysFs: '%s' for %s already has expected value of %s", sysFsPath, identifier, expectedThrottle
            );
        }
    }

    private String getMajorMinor(VlmProviderObject<Resource> vlmDataRef) throws StorageException
    {
        String majMin = deviceMajorMinorMap.get(vlmDataRef);
        if (vlmDataRef.exists())
        {
            if (majMin == null)
            {
                majMin = queryMajMin(extCmdFactory, vlmDataRef.getDevicePath());
                deviceMajorMinorMap.put(vlmDataRef, majMin);
            }
        }
        else
        {
            deviceMajorMinorMap.remove(vlmDataRef);
        }

        return majMin;
    }

    private void execForAllVlmData(
        AbsRscLayerObject<Resource> rscLayerData,
        boolean recursive,
        Executor<VlmProviderObject<Resource>> consumer
    )
        throws StorageException, AccessDeniedException, InvalidKeyException
    {
        for (VlmProviderObject<Resource> vlmData : rscLayerData.getVlmLayerObjects().values())
        {
            consumer.exec(vlmData);
        }
        if (recursive)
        {
            for (AbsRscLayerObject<Resource> childRscData : rscLayerData.getChildren())
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

    private void setSpdkIO(String path, String key, String data) throws StorageException
    {
            String parameter = SPDK_THROTTLE_READ_MBPS;
            if (ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_WRITE.equals(key))
            {
                parameter = SPDK_THROTTLE_WRITE_MBPS;
            }

            Commands.genericExecutor(
                extCmdFactory.create(),
                new String[]
                {
                    SPDK_RPC_SCRIPT,
                    "set_bdev_qos_limit",
                    path.split(SPDK_PATH_PREFIX)[1],
                    parameter,
                    String.valueOf(Integer.valueOf(data) / 1024 / 1024) // bytes to megabytes
                },
                "Failed to set " + key + " of device " + path,
                "Failed to set " + key + " of device " + path
            );
    }

    public static String queryMajMin(ExtCmdFactory extCmdFactory, String devicePath) throws StorageException
    {
        String majMin = null;
        if (devicePath != null)
        {
            OutputData outputData = Commands.genericExecutor(
                extCmdFactory.create(),
                new String[]
                {
                    "stat",
                    "-L", // follow links
                    "-c", "%t:%T",
                    devicePath
                },
                "Failed to find major:minor of device " + devicePath,
                "Failed to find major:minor of device " + devicePath,
                new RetryHandler()
                {
                    int retryCount = 3;
                    @Override
                    public boolean skip(OutputData outDataRef)
                    {
                        return false;
                    }

                    @Override
                    public boolean retry(OutputData outputDataRef)
                    {
                        final boolean retryFlag = retryCount > 0;
                        if (retryFlag)
                        {
                            --retryCount;
                            try
                            {
                                Thread.sleep(100);
                            }
                            catch (InterruptedException exc)
                            {
                            }
                        }
                        return retryFlag;
                    }
                }
            );
            majMin = new String(outputData.stdoutData).trim();
            String[] split = majMin.split(":");
            String major = Integer.toString(Integer.parseInt(split[0], BASE_HEX));
            String minor = Long.toString(Long.parseLong(split[1], BASE_HEX));
            majMin = major + ":" + minor;
        }
        return majMin;
    }

    public static Map<String, String> queryUevent(ExtCmdFactory extCmdFactory, String majMin) throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                "cat",
                "/sys/dev/block/" + majMin + "/uevent"
            },
            "Failed to query uevent of device '" + majMin + "'",
            "Failed to query uevent of device '" + majMin + "'"
        );
        String outStr = new String(outputData.stdoutData);
        Map<String, String> ret = new LinkedHashMap<>();

        String[] lines = outStr.split("\n");
        for (String line : lines)
        {
            String[] parts = line.trim().split("=");
            ret.put(parts[0], parts[1]);
        }

        return ret;
    }

    public static boolean queryDaxSupport(ExtCmdFactory extCmdFactoryRef, String block) throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create(),
            new String[]
            {
                "cat",
                "/sys/block/" + block + "/queue/dax"
            },
            "Failed to query device '" + block + "' for dax support",
            "Failed to query device '" + block + "' for dax support"
        );
        String out = new String(outputData.stdoutData).trim();
        return out.equals("1");
    }

    public HashMap<UUID, String> queryBCacheDevices() throws StorageException
    {
        HashMap<UUID, String> ret = new HashMap<>();
        try
        {
            Files.walkFileTree(
                PATH_SYS_FS_BCACHE,
                new SysFsFileWalker(
                    (path, ignored) ->
                    {
                        String pathStr = path.toString();
                        Matcher matcher = PATTERN_PATH_SYS_FS_BCACHE_BDEV.matcher(pathStr);
                        Path pathDev = path.resolve("dev");
                        if (matcher.find() && Files.exists(pathDev))
                        {
                            String link = Files.readSymbolicLink(pathDev).getFileName().toString();
                            errorReporter.logTrace("SysFsHandler: Found BCache: %s -> %s", matcher.group(1), link);
                            ret.put(
                                UUID.fromString(matcher.group(1)),
                                link
                            );
                        }
                        return FileVisitResult.CONTINUE;
                }
                ));
            if (ret.isEmpty())
            {
                errorReporter.logTrace("SysFsHandler: No BCache found");
            }
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to query bcache device paths", exc);
        }
        return ret;
    }

    private interface Executor<T>
    {
        void exec(T obj) throws StorageException, AccessDeniedException, InvalidKeyException;
    }

    private interface BiExecutor<T, V>
    {
        void exec(T obj, V arg2) throws StorageException, AccessDeniedException, InvalidKeyException;
    }

    private static class SysFsFileWalker extends SimpleFileVisitor<Path>
    {
        private final ExceptionThrowingBiFunction<Path, BasicFileAttributes, FileVisitResult, IOException> fct;

        public SysFsFileWalker(
            ExceptionThrowingBiFunction<Path, BasicFileAttributes, FileVisitResult, IOException> fctRef
        )
        {
            fct = fctRef;
        }

        @Override
        public FileVisitResult visitFile(Path fileRef, BasicFileAttributes attrsRef) throws IOException
        {
            return fct.accept(fileRef, attrsRef);
        }
    }
}
