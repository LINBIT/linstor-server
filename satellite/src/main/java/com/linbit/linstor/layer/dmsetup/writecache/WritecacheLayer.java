package com.linbit.linstor.layer.dmsetup.writecache;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.dmsetup.DmSetupUtils;
import com.linbit.linstor.layer.storage.DeviceProviderMapper;
import com.linbit.linstor.layer.storage.utils.MkfsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class WritecacheLayer implements DeviceLayer
{
    private static final String FORMAT_DM_NAME = "linstor_writecache_%s%s_%05d";
    private static final String FORMAT_DEV_PATH = "/dev/mapper/%s";

    /**
     * Maps the ApiConsts.KEY_WRITECACHE_OPTS* to the actual argument keys
     */
    private static final Map<String, String> OPTS_LUT;
    private static final String DFLT_CACHE_SIZE = "5%";

    private final ErrorReporter errorReporter;
    private final AccessContext storDriverAccCtx;
    private final DeviceProviderMapper deviceProviderMapper;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;

    private Props localNodeProps;
    private StltConfigAccessor stltConfAccessor;

    static
    {
        OPTS_LUT = new TreeMap<>();
        OPTS_LUT.put(ApiConsts.KEY_WRITECACHE_OPTION_START_SECTOR, "start_sector");
        OPTS_LUT.put(ApiConsts.KEY_WRITECACHE_OPTION_HIGH_WATERMARK, "high_watermark");
        OPTS_LUT.put(ApiConsts.KEY_WRITECACHE_OPTION_LOW_WATERMARK, "low_watermark");
        OPTS_LUT.put(ApiConsts.KEY_WRITECACHE_OPTION_WRITEBACK_JOBS, "writeback_jobs");
        OPTS_LUT.put(ApiConsts.KEY_WRITECACHE_OPTION_AUTOCOMMIT_BLOCKS, "autocommit_blocks");
        OPTS_LUT.put(ApiConsts.KEY_WRITECACHE_OPTION_AUTOCOMMIT_TIME, "autocommit_time");
        // special handling for KEY_WRITE_CAHCE_OPTION_FUA
    }

    @Inject
    public WritecacheLayer(
        ErrorReporter errorReporterRef,
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        DeviceProviderMapper deviceProviderMapperRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorProviderRef,
        StltConfigAccessor stltConfAccessorRef
    )
    {
        errorReporter = errorReporterRef;
        storDriverAccCtx = storDriverAccCtxRef;
        deviceProviderMapper = deviceProviderMapperRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorProviderRef;
        stltConfAccessor = stltConfAccessorRef;
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }


    @Override
    public void prepare(
        Set<AbsRscLayerObject<Resource>> rscObjListRef,
        Set<AbsRscLayerObject<Snapshot>> snapObjListRef
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        errorReporter.logTrace("Writecache: listing all 'writecache' devices");
        Set<String> dmDeviceNames = DmSetupUtils.list(extCmdFactory.create(), "writecache");

        for (AbsRscLayerObject<Resource> rscDataObj : rscObjListRef)
        {
            WritecacheRscData<Resource> rscData = (WritecacheRscData<Resource>) rscDataObj;
            for (WritecacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                String devName = getIdentifier(vlmData);
                boolean exists = false;
                for (String dmDev : dmDeviceNames)
                {
                    if (dmDev.equals(devName))
                    {
                        exists = true;
                        break;
                    }
                }
                String identifier = devName;

                vlmData.setExists(exists);
                vlmData.setIdentifier(identifier);
                if (!exists)
                {
                    errorReporter.logTrace(
                        "Writecache: device not found. Will be created for identifier: %s",
                        identifier
                    );
                    vlmData.setDevicePath(null);
                }
                else
                {
                    errorReporter.logTrace("Writecache: device exists: %s, nothing to do", devName);
                    vlmData.setDevicePath(
                        String.format(
                            FORMAT_DEV_PATH,
                            identifier
                        )
                    );
                }
            }
        }
    }

    private String getIdentifier(VlmProviderObject<Resource> vlmDataRef)
    {
        AbsRscLayerObject<Resource> rscData = vlmDataRef.getRscLayerObject();
        return String.format(
            FORMAT_DM_NAME,
            rscData.getResourceName().displayValue,
            rscData.getResourceNameSuffix(),
            vlmDataRef.getVlmNr().value
        );
    }

    @Override
    public void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        updateSize((WritecacheVlmData<Resource>) vlmData, true);

    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        updateSize((WritecacheVlmData<Resource>) vlmData, false);
    }

    private void updateSize(WritecacheVlmData<Resource> vlmData, boolean fromUsable)
        throws AccessDeniedException, DatabaseException
    {
        VlmProviderObject<Resource> dataChildVlmData = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
        VlmProviderObject<Resource> cacheChildVlmData = vlmData.getChildBySuffix(
            RscLayerSuffixes.SUFFIX_WRITECACHE_CACHE
        );

        if (fromUsable)
        {
            dataChildVlmData.setUsableSize(vlmData.getUsableSize());
            resourceProcessorProvider.get().updateAllocatedSizeFromUsableSize(dataChildVlmData);
        }
        else
        {
            dataChildVlmData.setAllocatedSize(vlmData.getAllocatedSize());
            resourceProcessorProvider.get().updateUsableSizeFromAllocatedSize(dataChildVlmData);
        }

        long cacheSize;
        if (cacheChildVlmData != null)
        {
            // null if we are above an NVMe target
            String cacheSizeStr = getCacheSize(vlmData.getVolume());
            if (cacheSizeStr.endsWith("%"))
            {
                String cacheSizePercent = cacheSizeStr.substring(0, cacheSizeStr.length() - 1);
                double percent = Double.parseDouble(cacheSizePercent) / 100;
                cacheSize = Math.round(percent * vlmData.getUsableSize() + 0.5);
            }
            else
            {
                cacheSize = Long.parseLong(cacheSizeStr);
            }

            // even if we are updating fromAllocated, cache device still need to be calculated fromUsable
            cacheChildVlmData.setUsableSize(cacheSize);
            resourceProcessorProvider.get().updateAllocatedSizeFromUsableSize(cacheChildVlmData);
        }
        else
        {
            cacheSize = 0;
        }

        if (!fromUsable)
        {
            vlmData.setUsableSize(vlmData.getAllocatedSize());
        }
        vlmData.setAllocatedSize(vlmData.getUsableSize() + cacheSize);
    }

    @Override
    public LayerProcessResult process(
        AbsRscLayerObject<Resource> rscLayerDataRef,
        List<Snapshot> snapshotListRef,
        ApiCallRcImpl apiCallRcRef
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        LayerProcessResult ret;
        WritecacheRscData<Resource> rscData = (WritecacheRscData<Resource>) rscLayerDataRef;
        StateFlags<Flags> rscFlags = rscData.getAbsResource().getStateFlags();
        boolean deleteFlagSet = rscFlags.isSet(storDriverAccCtx, Resource.Flags.DELETE) ||
            rscFlags.isSet(storDriverAccCtx, Resource.Flags.INACTIVE);
        if (deleteFlagSet)
        {
            for (WritecacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                if (vlmData.exists())
                {
                    errorReporter.logDebug(
                        "Writecache: removing %s (%s)",
                        vlmData.getIdentifier(),
                        vlmData.getDevicePath()
                    );
                    DmSetupUtils.remove(extCmdFactory.create(), vlmData.getIdentifier());
                    vlmData.setExists(false);
                    vlmData.setDevicePath(null);
                }
                else
                {
                    errorReporter.logDebug(
                        "Writecache: noop when removing %s (%s), as it does not exist",
                        vlmData.getIdentifier(),
                        vlmData.getDevicePath()
                    );
                }
            }
        }

        if (rscLayerDataRef.getAbsResource().getLayerData(storDriverAccCtx).getSuspendIo())
        {
            /*
             * rsc.getLayerData is either the same reference as our local rscLayerDataRef OR
             * it is the root-layerData.
             * In case of suspendIO, only the root-layerData gets the suspendIO set.
             * However, even if root-layerData != rscLayerDataRef, we still want to flush
             * our write-cache.
             */
            for (VlmProviderObject<Resource> vlmData : rscLayerDataRef.getVlmLayerObjects().values())
            {
                if (vlmData.exists())
                {
                    DmSetupUtils.flush(extCmdFactory, vlmData.getDevicePath());
                }
            }
        }

        LayerProcessResult dataResult = resourceProcessorProvider.get().process(
            rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA),
            snapshotListRef,
            apiCallRcRef
        );
        LayerProcessResult metaResult;
        AbsRscLayerObject<Resource> cacheRscChild = rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_WRITECACHE_CACHE);
        if (cacheRscChild == null)
        {
            // we might be an imaginary layer above an NVMe target which does not need a cache...
            errorReporter.logDebug("Writecache: no devices provided to upper layer");
            metaResult = LayerProcessResult.NO_DEVICES_PROVIDED;
        }
        else
        {
            metaResult = resourceProcessorProvider.get().process(
                cacheRscChild,
                snapshotListRef,
                apiCallRcRef
            );
        }

        if (!deleteFlagSet && dataResult == LayerProcessResult.SUCCESS && metaResult == LayerProcessResult.SUCCESS)
        {
            for (WritecacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                PriorityProps prioProps;
                {
                    Volume vlm = (Volume) vlmData.getVolume();
                    ResourceDefinition rscDfn = vlm.getResourceDefinition();
                    ResourceGroup rscGrp = rscDfn.getResourceGroup();
                    prioProps = new PriorityProps(
                        vlm.getProps(storDriverAccCtx),
                        vlm.getVolumeDefinition().getProps(storDriverAccCtx),
                        rscDfn.getProps(storDriverAccCtx),
                        rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmData.getVlmNr()),
                        rscGrp.getProps(storDriverAccCtx),
                        localNodeProps,
                        stltConfAccessor.getReadonlyProps()
                    );
                }
                if (!vlmData.exists())
                {
                    VlmProviderObject<Resource> dataChild = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
                    VlmProviderObject<Resource> cacheChild = vlmData.getChildBySuffix(
                        RscLayerSuffixes.SUFFIX_WRITECACHE_CACHE
                    );
                    boolean considerCacheAsPmem = false;
                    {
                        boolean allStorPoolsPmem = true;
                        boolean onePmem = false;
                        Set<StorPool> storPoolSet = LayerVlmUtils.getStorPoolSet(cacheChild, storDriverAccCtx);
                        for (StorPool storPool : storPoolSet)
                        {
                            if (storPool.isPmem())
                            {
                                onePmem = true;
                            }
                            else
                            {
                                allStorPoolsPmem = false;
                            }
                        }
                        errorReporter.logTrace(
                            "Writecache: at least one pmem: %s, all pmem: %s",
                            onePmem,
                            allStorPoolsPmem
                        );

                        if (!allStorPoolsPmem && onePmem)
                        {
                            apiCallRcRef.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.MASK_VLM | ApiConsts.MASK_CRT | ApiConsts.WARN_MIXED_PMEM_AND_NON_PMEM,
                                    "Some storage pools are based on pmem while others are not. Falling back to 's' mode."
                                )
                            );
                        }
                        if (allStorPoolsPmem)
                        {
                            considerCacheAsPmem = true;
                        }
                        errorReporter.logDebug(
                            "Writecache: considering used storage pool%s as pmem: %s",
                            storPoolSet.size() == 1 ?
                               " [" + storPoolSet.iterator().next().getName().displayValue +"]" :
                                "s " + storPoolSet,
                            considerCacheAsPmem
                        );
                    }

                    Map<String, String> dmsetupOptsMap = prioProps.renderRelativeMap(
                        ApiConsts.NAMESPC_WRITECACHE_OPTIONS
                    );

                    StringBuilder dmsetupOptsSb = new StringBuilder();
                    for (Entry<String, String> entry : dmsetupOptsMap.entrySet())
                    {
                        String key = null;
                        String value = null;
                        switch (entry.getKey())
                        {
                            case ApiConsts.KEY_WRITECACHE_OPTION_ADDITIONAL:
                                key = entry.getValue();
                                // no value
                                break;
                            case ApiConsts.KEY_WRITECACHE_OPTION_FUA:
                                if (entry.getValue().equals(ApiConsts.VAL_WRITECACHE_FUA_ON))
                                {
                                    key = "fua";
                                }
                                else
                                {
                                    key = "nofua";
                                }
                                // no value
                                break;
                            default:
                                key = OPTS_LUT.get(entry.getKey());
                                value = entry.getValue();
                                break;
                        }
                        dmsetupOptsSb.append(key).append(" ");
                        if (value != null)
                        {
                            dmsetupOptsSb.append(value).append(" ");
                        }
                    }
                    if (dmsetupOptsSb.length() > 0)
                    {
                        dmsetupOptsSb.setLength(dmsetupOptsSb.length() - 1); // cut the last " "
                    }
                    String dmsetupOptsStr = dmsetupOptsSb.toString();
                    int dmsetupOpsCounts = MkfsUtils.shellSplit(dmsetupOptsStr).size();

                    DmSetupUtils.createWritecache(
                        extCmdFactory,
                        vlmData.getIdentifier(),
                        dataChild.getDevicePath(),
                        cacheChild.getDevicePath(),
                        considerCacheAsPmem,
                        4096,
                        dmsetupOpsCounts + " " + dmsetupOptsStr
                    );

                    vlmData.setDevicePath(
                        String.format(
                            FORMAT_DEV_PATH,
                            vlmData.getIdentifier()
                        )
                    );
                    vlmData.setExists(true);
                    errorReporter.logDebug("Writecache: device (%s) created", vlmData.getDevicePath());
                }
            }
            ret = LayerProcessResult.SUCCESS;
        }
        else
        {
            ret = LayerProcessResult.NO_DEVICES_PROVIDED;
        }
        return ret;
    }

    @Override
    public void clearCache() throws StorageException
    {
        // noop
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    @Override
    public void resourceFinished(AbsRscLayerObject<Resource> layerDataRef) throws AccessDeniedException
    {
        StateFlags<Flags> rscFlags = layerDataRef.getAbsResource().getStateFlags();
        if (rscFlags.isSet(storDriverAccCtx, Resource.Flags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(layerDataRef);
        }
        else
        {
            boolean isActive = rscFlags.isUnset(storDriverAccCtx, Resource.Flags.INACTIVE);
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                layerDataRef,
                new UsageState(
                    isActive,
                    null, // will be mapped to unknown
                    isActive
                )
            );
        }
    }

    private String getCacheSize(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getPrioProps(vlmRef).getProp(
            ApiConsts.KEY_WRITECACHE_SIZE, ApiConsts.NAMESPC_WRITECACHE, DFLT_CACHE_SIZE
        ).trim();
    }

    private PriorityProps getPrioProps(AbsVolume<Resource> vlmRef) throws AccessDeniedException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmRef.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        Resource rsc = vlmRef.getAbsResource();
        return new PriorityProps(
            vlmDfn.getProps(storDriverAccCtx),
            rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmDfn.getVolumeNumber()),
            rsc.getProps(storDriverAccCtx),
            rscDfn.getProps(storDriverAccCtx),
            rscGrp.getProps(storDriverAccCtx),
            rsc.getNode().getProps(storDriverAccCtx)
        );
    }
}
