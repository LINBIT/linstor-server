package com.linbit.linstor.layer.dmsetup.cache;

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
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.dmsetup.DmSetupUtils;
import com.linbit.linstor.layer.storage.utils.MkfsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CacheLayer implements DeviceLayer
{
    private static final String FORMAT_DM_NAME = "linstor_cache_%s%s_%05d";
    private static final String FORMAT_DEV_PATH = "/dev/mapper/%s";

    private static final String DFLT_CACHE_SIZE = "5%";
    private static final String DFLT_META_SIZE = "12288"; // 12M
    private static final String DFLT_BLOCK_SIZE = "4096";
    private static final String DFLT_FEATURE = "writethrough";
    private static final String DFLT_POLICY = "smq";

    private final ErrorReporter errorReporter;
    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;

    private Props localNodeProps;
    private StltConfigAccessor stltConfAccessor;

    @Inject
    public CacheLayer(
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
        errorReporter.logTrace("Cache: listing all 'cache' devices");
        Set<String> dmDeviceNames = DmSetupUtils.list(extCmdFactory.create(), "cache");

        for (AbsRscLayerObject<Resource> rscDataObj : rscObjListRef)
        {
            CacheRscData<Resource> rscData = (CacheRscData<Resource>) rscDataObj;
            for (CacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
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
                        "Cache: device not found. Will be created for identifier: %s",
                        identifier
                    );
                    vlmData.setDevicePath(null);
                }
                else
                {
                    errorReporter.logTrace("Cache: device exists: %s, nothing to do", devName);
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
        throws AccessDeniedException, DatabaseException, StorageException
    {
        updateSize((CacheVlmData<Resource>) vlmData, true);
    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        updateSize((CacheVlmData<Resource>) vlmData, false);
    }

    private void updateSize(
        CacheVlmData<Resource> vlmData,
        boolean fromUsable
    )
        throws AccessDeniedException, DatabaseException, StorageException
    {
        VlmProviderObject<Resource> dataChildVlmData = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
        VlmProviderObject<Resource> cacheChildVlmData = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_CACHE);
        VlmProviderObject<Resource> metaChildVlmData = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_META);

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
        long metaSize;
        if (cacheChildVlmData != null && metaChildVlmData != null)
        {
            // null if we are above an NVMe target
            String cacheSizeStr = getCacheSize(vlmData.getVolume());
            cacheSize = calcSize(vlmData, cacheSizeStr);

            String metaSizeStr = getMetaSize(vlmData.getVolume());
            metaSize = calcSize(vlmData, metaSizeStr);

            // even if we are updating fromAllocated, cache device still need to be calculated fromUsable
            cacheChildVlmData.setUsableSize(cacheSize);
            resourceProcessorProvider.get().updateAllocatedSizeFromUsableSize(cacheChildVlmData);

            metaChildVlmData.setUsableSize(cacheSize);
            resourceProcessorProvider.get().updateAllocatedSizeFromUsableSize(metaChildVlmData);
        }
        else
        {
            cacheSize = 0;
            metaSize = 0;
        }

        if (!fromUsable)
        {
            vlmData.setUsableSize(vlmData.getAllocatedSize());
        }
        vlmData.setAllocatedSize(vlmData.getUsableSize() + cacheSize + metaSize);
    }

    private long calcSize(CacheVlmData<Resource> vlmData, String sizeStr)
    {
        long cacheSize;
        if (sizeStr.endsWith("%"))
        {
            String cacheSizePercent = sizeStr.substring(0, sizeStr.length() - 1);
            double percent = Double.parseDouble(cacheSizePercent) / 100;
            cacheSize = Math.round(percent * vlmData.getUsableSize() + 0.5);
        }
        else
        {
            cacheSize = Long.parseLong(sizeStr);
        }
        return cacheSize;
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
        CacheRscData<Resource> rscData = (CacheRscData<Resource>) rscLayerDataRef;
        StateFlags<Flags> rscFlags = rscData.getAbsResource().getStateFlags();
        boolean deleteFlagSet = rscFlags.isSet(storDriverAccCtx, Resource.Flags.DELETE) ||
            rscFlags.isSet(storDriverAccCtx, Resource.Flags.INACTIVE);
        if (deleteFlagSet)
        {
            for (CacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                if (vlmData.exists())
                {
                    errorReporter.logDebug(
                        "Cache: removing %s (%s)",
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
                        "Cache: noop when removing %s (%s), as it does not exist",
                        vlmData.getIdentifier(),
                        vlmData.getDevicePath()
                    );
                }
            }
        }

        LayerProcessResult dataResult = resourceProcessorProvider.get().process(
            rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA),
            snapshotListRef,
            apiCallRcRef
        );
        LayerProcessResult metaResult;
        LayerProcessResult cacheResult;
        AbsRscLayerObject<Resource> cacheRscChild = rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_CACHE);
        AbsRscLayerObject<Resource> metaRscChild = rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_META);
        if (cacheRscChild == null || metaRscChild == null)
        {
            // we might be an imaginary layer above an NVMe target which does not need a cache...
            errorReporter.logDebug("Cache: no devices provided to upper layer");
            cacheResult = LayerProcessResult.NO_DEVICES_PROVIDED;
            metaResult = LayerProcessResult.NO_DEVICES_PROVIDED;
        }
        else
        {
            cacheResult = resourceProcessorProvider.get().process(
                cacheRscChild,
                snapshotListRef,
                apiCallRcRef
            );
            metaResult = resourceProcessorProvider.get().process(
                metaRscChild,
                snapshotListRef,
                apiCallRcRef
            );
        }

        if (
            !deleteFlagSet &&
                dataResult == LayerProcessResult.SUCCESS &&
                cacheResult == LayerProcessResult.SUCCESS &&
                metaResult == LayerProcessResult.SUCCESS
        )
        {
            for (CacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                Volume vlm = (Volume) vlmData.getVolume();
                PriorityProps prioProps;
                {
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
                        RscLayerSuffixes.SUFFIX_CACHE_CACHE
                    );
                    VlmProviderObject<Resource> metaChild = vlmData.getChildBySuffix(
                        RscLayerSuffixes.SUFFIX_CACHE_META
                    );

                    Map<String, String> dmsetupPolicyArgsMap = prioProps.renderRelativeMap(
                        ApiConsts.NAMESPC_CACHE_POLICY_ARGS
                    );

                    String policyArgStr = ""; // for now
                    int policyArgsCount = MkfsUtils.shellSplit(policyArgStr).size();
                    DmSetupUtils.createCache(
                        extCmdFactory,
                        vlmData.getIdentifier(),
                        dataChild.getDevicePath(),
                        cacheChild.getDevicePath(),
                        metaChild.getDevicePath(),
                        getBlocksize(vlm),
                        getFeature(vlm),
                        getPolicy(vlm),
                        policyArgsCount + " " + policyArgStr
                    );

                    vlmData.setDevicePath(
                        String.format(
                            FORMAT_DEV_PATH,
                            vlmData.getIdentifier()
                        )
                    );
                    vlmData.setExists(true);
                    errorReporter.logDebug("Cache: device (%s) created", vlmData.getDevicePath());
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
    public LocalPropsChangePojo setLocalNodeProps(Props localNodePropsRef)
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
                layerDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_CACHE);
            AbsRscLayerObject<Resource> metaRscChild =
                layerDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_META);

            if (cacheRscChild == null && metaRscChild == null)
            {
                // we are above an nvme-target
                resourceReadySent = false;
            }
            else
            {
                boolean isActive = rscFlags.isUnset(storDriverAccCtx, Resource.Flags.INACTIVE);
                resourceProcessorProvider.get().sendResourceCreatedEvent(
                    layerDataRef,
                    new ResourceState(
                        isActive,
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

    private String getCacheSize(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getProp(
            vlmRef,
            ApiConsts.KEY_CACHE_CACHE_SIZE,
            DFLT_CACHE_SIZE
        );
    }

    private String getMetaSize(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getProp(
            vlmRef,
            ApiConsts.KEY_CACHE_META_SIZE,
            DFLT_META_SIZE
        );
    }

    private long getBlocksize(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return Long.parseLong(
            getProp(
                vlmRef,
                ApiConsts.KEY_CACHE_META_SIZE,
                DFLT_BLOCK_SIZE
            )
        );
    }

    private String getFeature(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getProp(
            vlmRef,
            ApiConsts.KEY_CACHE_OPERATING_MODE,
            DFLT_FEATURE
        );
    }

    private String getPolicy(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getProp(
            vlmRef,
            ApiConsts.KEY_CACHE_POLICY,
            DFLT_POLICY
        );
    }

    private String getProp(AbsVolume<Resource> vlmRef, String key, String dfltValue)
        throws InvalidKeyException, AccessDeniedException
    {
        return getPrioProps(vlmRef).getProp(
            key,
            ApiConsts.NAMESPC_CACHE,
            dfltValue
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
            localNodeProps,
            stltConfAccessor.getReadonlyProps()
        );
    }
}
