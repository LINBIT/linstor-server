package com.linbit.linstor.layer.dmsetup.cache;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
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
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.cache.CacheLayerSizeCalculator;
import com.linbit.linstor.layer.dmsetup.DmSetupUtils;
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
import com.linbit.linstor.storage.utils.MkfsUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class CacheLayer implements DeviceLayer
{
    private static final String FORMAT_DM_NAME = "linstor_cache_%s%s_%05d";
    private static final String FORMAT_DEV_PATH = "/dev/mapper/%s";

    private static final String DFLT_FEATURE = "writeback";
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
    public boolean isSuspendIoSupported()
    {
        return true;
    }

    @Override
    public void suspendIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        DmSetupUtils.suspendIo(errorReporter, extCmdFactory, rscDataRef, true, null);
    }

    @Override
    public void resumeIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        DmSetupUtils.suspendIo(errorReporter, extCmdFactory, rscDataRef, false, null);
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
        CacheRscData<Resource> rscData = (CacheRscData<Resource>) rscLayerDataRef;
        StateFlags<Flags> rscFlags = rscData.getAbsResource().getStateFlags();
        boolean shouldRscExist = rscFlags.isUnset(
            storDriverAccCtx,
            Resource.Flags.DELETE,
            Resource.Flags.INACTIVE,
            Resource.Flags.DISK_REMOVING
        );
        for (CacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
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

        resourceProcessorProvider.get().processResource(
            rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA),
            apiCallRcRef
        );
        AbsRscLayerObject<Resource> cacheRscChild = rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_CACHE);
        AbsRscLayerObject<Resource> metaRscChild = rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_META);
        if (cacheRscChild == null || metaRscChild == null)
        {
            // we might be an imaginary layer above an NVMe target which does not need a cache...
            errorReporter.logDebug("Cache: no devices provided to upper layer");
        }
        else
        {
            resourceProcessorProvider.get().processResource(
                cacheRscChild,
                apiCallRcRef
            );
            resourceProcessorProvider.get().processResource(
                metaRscChild,
                apiCallRcRef
            );
        }

        if (shouldRscExist)
        {
            for (CacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                Volume vlm = (Volume) vlmData.getVolume();
                boolean shouldVlmExist = vlm.getFlags().isUnset(
                    storDriverAccCtx,
                    Volume.Flags.DELETE,
                    Volume.Flags.CLONING
                ) && vlm.getVolumeDefinition().getFlags().isUnset(
                    storDriverAccCtx,
                    VolumeDefinition.Flags.DELETE
                );
                if (shouldVlmExist)
                {
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
            }
        }
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

    private long getBlocksize(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        @Nullable String propValue = getCacheProp(vlmRef, ApiConsts.KEY_CACHE_BLOCK_SIZE);
        long ret;
        if (propValue == null)
        {
            ret = CacheLayerSizeCalculator.DFLT_BLOCK_SIZE_IN_BYTES;
        }
        else
        {
            ret = Long.parseLong(propValue);
        }
        return ret;
    }

    private String getFeature(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getCacheProp(vlmRef, ApiConsts.KEY_CACHE_OPERATING_MODE, DFLT_FEATURE);
    }

    private String getPolicy(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getCacheProp(vlmRef, ApiConsts.KEY_CACHE_POLICY, DFLT_POLICY);
    }

    private @Nullable String getCacheProp(AbsVolume<Resource> vlmRef, String key)
        throws InvalidKeyException, AccessDeniedException
    {
        @Nullable String value = getPrioProps(vlmRef).getProp(key, ApiConsts.NAMESPC_CACHE);
        return value == null ? null : value.trim();
    }

    private String getCacheProp(AbsVolume<Resource> vlmRef, String key, String dfltValue)
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

    @Override
    public CloneSupportResult getCloneSupport(
        AbsRscLayerObject<?> ignoredSourceRef,
        AbsRscLayerObject<?> ignoredTargetRef
    )
    {
        return CloneSupportResult.PASSTHROUGH;
    }

}
