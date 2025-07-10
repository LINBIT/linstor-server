package com.linbit.linstor.layer.dmsetup.writecache;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.dmsetup.DmSetupUtils;
import com.linbit.linstor.logging.ErrorReporter;
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
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.ShellUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Collections;
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

    private final ErrorReporter errorReporter;
    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;

    private @Nullable Props localNodeProps;
    private final StltConfigAccessor stltConfAccessor;

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
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorProviderRef,
        StltConfigAccessor stltConfAccessorRef
    )
    {
        errorReporter = errorReporterRef;
        storDriverAccCtx = storDriverAccCtxRef;
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
                        "Writecache: device not found for identifier: %s",
                        identifier
                    );
                    vlmData.setDevicePath(null);
                }
                else
                {
                    errorReporter.logTrace("Writecache: device exists: %s", devName);
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
    public boolean isSuspendIoSupported()
    {
        return true;
    }

    @Override
    public void suspendIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        DmSetupUtils.suspendIo(
            errorReporter,
            extCmdFactory,
            rscDataRef,
            true,
            vlmData -> DmSetupUtils.flushOnSuspend(
                extCmdFactory,
                vlmData
            )
        );
    }

    @Override
    public void resumeIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        DmSetupUtils.suspendIo(errorReporter, extCmdFactory, rscDataRef, false, null);
    }

    @Override
    public void managePostRootSuspend(AbsRscLayerObject<Resource> rscDataRef) throws StorageException
    {
        if (!rscDataRef.hasAnyPreventExecutionIgnoreReason())
        {
            DmSetupUtils.flush(extCmdFactory, rscDataRef);
        }
    }

    @Override
    public void updateSuspendState(AbsRscLayerObject<Resource> rscDataRef)
        throws DatabaseException, ExtCmdFailedException
    {
        rscDataRef.setIsSuspended(DmSetupUtils.isSuspended(extCmdFactory, rscDataRef));
    }

    @Override
    public void processResource(
        AbsRscLayerObject<Resource> rscLayerDataRef,
        ApiCallRcImpl apiCallRcRef
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        WritecacheRscData<Resource> rscData = (WritecacheRscData<Resource>) rscLayerDataRef;
        StateFlags<Flags> rscFlags = rscData.getAbsResource().getStateFlags();
        boolean shouldRscExist = rscFlags.isUnset(
            storDriverAccCtx,
            Resource.Flags.DELETE,
            Resource.Flags.INACTIVE,
            Resource.Flags.DISK_REMOVING
        );
        for (WritecacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
        {
            boolean shouldVlmExist = ((Volume) vlmData.getVolume()).getFlags().isUnset(
                storDriverAccCtx,
                Volume.Flags.DELETE,
                Volume.Flags.CLONING
            ) && vlmData.getVolume().getVolumeDefinition().getFlags().isUnset(
                storDriverAccCtx,
                VolumeDefinition.Flags.DELETE
            );
            if (!shouldRscExist || !shouldVlmExist)
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

        resourceProcessorProvider.get().processResource(
            rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA),
            apiCallRcRef
        );
        AbsRscLayerObject<Resource> cacheRscChild = rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_WRITECACHE_CACHE);
        if (cacheRscChild == null)
        {
            // we might be an imaginary layer above an NVMe target which does not need a cache...
            errorReporter.logDebug("Writecache: no devices provided to upper layer");
        }
        else
        {
            resourceProcessorProvider.get().processResource(cacheRscChild,apiCallRcRef);
        }

        if (shouldRscExist)
        {
            for (WritecacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                boolean shouldVlmExist = ((Volume) vlmData.getVolume()).getFlags().isUnset(
                    storDriverAccCtx,
                    Volume.Flags.DELETE,
                    Volume.Flags.CLONING
                ) && vlmData.getVolume().getVolumeDefinition().getFlags().isUnset(
                    storDriverAccCtx,
                    VolumeDefinition.Flags.DELETE
                );
                if (shouldVlmExist)
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
                                        ApiConsts.MASK_VLM | ApiConsts.MASK_CRT |
                                            ApiConsts.WARN_MIXED_PMEM_AND_NON_PMEM,
                                        "Some storage pools are based on pmem while others are not. " +
                                            "Falling back to 's' mode."
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
                                    " [" + storPoolSet.iterator().next().getName().displayValue + "]" :
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
                        int dmsetupOpsCounts = ShellUtils.shellSplit(dmsetupOptsStr).size();

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
            }
        }
    }

    @Override
    public void clearCache() throws StorageException
    {
        // noop
    }

    @Override
    public @Nullable LocalPropsChangePojo setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
        return null;
    }

    @Override
    public boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef) throws AccessDeniedException
    {
        boolean resourceReadySent = true;

        StateFlags<Flags> rscFlags = layerDataRef.getAbsResource().getStateFlags();
        if (rscFlags.isSet(storDriverAccCtx, Resource.Flags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(layerDataRef);
        }
        else
        {
            AbsRscLayerObject<Resource> cacheRscChild =
                layerDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_WRITECACHE_CACHE);
            if (cacheRscChild == null)
            {
                // we might be an imaginary layer above an NVMe target which does not need a cache...
                resourceReadySent = false;
            }
            else
            {
                boolean isActive = rscFlags.isUnset(storDriverAccCtx, Resource.Flags.INACTIVE);
                resourceProcessorProvider.get().sendResourceCreatedEvent(
                    layerDataRef,
                    new ResourceState(
                        isActive,
                        // no (drbd) connections to peers
                        Collections.emptyMap(),
                        null, // will be mapped to unknown
                        isActive,
                        null,
                        null
                    )
                );
            }
        }
        return resourceReadySent;
    }

    @Override
    public boolean isDeleteFlagSet(AbsRscLayerObject<?> rscDataRef)
    {
        return false; // no layer specific DELETE flag
    }

    @Override
    public CloneSupportResult getCloneSupport(
        AbsRscLayerObject<?> ignoredSourceRef,
        AbsRscLayerObject<?> ignoredTargetRef
    )
    {
        return CloneSupportResult.PASSTHROUGH;
    }

    @Override
    public DeviceLayerKind getKind()
    {
        return DeviceLayerKind.WRITECACHE;
    }
}
