package com.linbit.linstor.layer.bcache;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.SysFsHandler;
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
import com.linbit.linstor.layer.dmsetup.DmSetupUtils;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Provider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

public class BCacheLayer implements DeviceLayer
{
    private static final long WAIT_FOR_BCACHE_TIMEOUT_MS = 100;
    private static final long WAIT_FOR_BCACHE_RETRY_COUNT = 20;

    private static final long BCACHE_METADATA_SIZE_IN_KIB = 8;

    private static final String DFLT_CACHE_SIZE = "5%";
    private static final long MIN_CACHE_SIZE_IN_KIB = 512 * 1024; // 512MB

    private final ErrorReporter errorReporter;
    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final StltConfigAccessor stltConfAccessor;
    private final WipeHandler wipeHandler;
    private final SysFsHandler sysFsHandler;

    private Props localNodeProps;

    @Inject
    public BCacheLayer(
        ErrorReporter errorReporterRef,
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorProviderRef,
        StltConfigAccessor stltConfAccessorRef,
        WipeHandler wipeHandlerRef,
        SysFsHandler sysFsHandlerRef
    )
    {
        errorReporter = errorReporterRef;
        storDriverAccCtx = storDriverAccCtxRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorProviderRef;
        stltConfAccessor = stltConfAccessorRef;
        wipeHandler = wipeHandlerRef;
        sysFsHandler = sysFsHandlerRef;
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }


    @Override
    public void prepare(Set<AbsRscLayerObject<Resource>> rscObjListRef, Set<AbsRscLayerObject<Snapshot>> snapObjListRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        errorReporter.logTrace("BCache: listing all bcache devices");
        HashMap<UUID, String> bcacheUuidToDev = sysFsHandler.queryBCacheDevices();

        for (AbsRscLayerObject<Resource> rscDataObj : rscObjListRef)
        {
            BCacheRscData<Resource> rscData = (BCacheRscData<Resource>) rscDataObj;
            for (BCacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                String identifier = bcacheUuidToDev.get(vlmData.getDeviceUuid());

                if (identifier == null)
                {
                    // uninitialized
                    identifier = null;
                    errorReporter.logTrace(
                        "BCache: device not found. Will be created for %s",
                        toString(vlmData)
                    );
                    vlmData.setIdentifier(null);
                    vlmData.setExists(false);
                    vlmData.setDevicePath(null);
                }
                else
                {
                    errorReporter.logTrace("BCache: device exists: %s, nothing to do", identifier);
                    vlmData.setIdentifier(identifier);
                    vlmData.setExists(true);
                    vlmData.setDevicePath(buildDevicePath(identifier));
                }

            }
        }
    }

    @Override
    public void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        updateSize((BCacheVlmData<Resource>) vlmData, true);
    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        updateSize((BCacheVlmData<Resource>) vlmData, false);
    }

    /*
     * TODO: method mostly copied from WritecacheLayer. Most certainly also quite similar to CacheLayer's calculation
     * We should think about refactoring those three into one..
     */
    private void updateSize(BCacheVlmData<Resource> vlmData, boolean fromUsable)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        VlmProviderObject<Resource> dataChildVlmData = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
        VlmProviderObject<Resource> cacheChildVlmData = vlmData.getChildBySuffix(
            RscLayerSuffixes.SUFFIX_BCACHE_CACHE
        );

        if (fromUsable)
        {
            dataChildVlmData.setUsableSize(vlmData.getUsableSize() + BCACHE_METADATA_SIZE_IN_KIB);
            resourceProcessorProvider.get().updateAllocatedSizeFromUsableSize(dataChildVlmData);
        }
        else
        {
            dataChildVlmData.setAllocatedSize(vlmData.getAllocatedSize() + BCACHE_METADATA_SIZE_IN_KIB);
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

            if (cacheSize < MIN_CACHE_SIZE_IN_KIB)
            {
                errorReporter.logDebug(
                    "BCache: size %dKiB was too small. Rounded up to %dKiB",
                    cacheSize,
                    MIN_CACHE_SIZE_IN_KIB
                );
                cacheSize = MIN_CACHE_SIZE_IN_KIB;
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
        vlmData.setAllocatedSize(vlmData.getUsableSize() + cacheSize + BCACHE_METADATA_SIZE_IN_KIB);
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
        managePostRootSuspend(rscDataRef); // also flush our data after suspending
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
    public void managePostRootSuspend(AbsRscLayerObject<Resource> rscDataRef) throws StorageException
    {
        for (VlmProviderObject<Resource> vlmData : rscDataRef.getVlmLayerObjects().values())
        {
            BCacheUtils.flush(
                errorReporter,
                ((BCacheVlmData<Resource>) vlmData).getIdentifier()
            );
        }
    }

    @Override
    public void process(
        AbsRscLayerObject<Resource> rscLayerDataRef,
        List<Snapshot> snapshotListRef,
        ApiCallRcImpl apiCallRcRef
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException,
        AbortLayerProcessingException
    {
        BCacheRscData<Resource> rscData = (BCacheRscData<Resource>) rscLayerDataRef;
        StateFlags<Flags> rscFlags = rscData.getAbsResource().getStateFlags();

        boolean deleteFlagSet = rscFlags.isSomeSet(storDriverAccCtx, Resource.Flags.DELETE);
        boolean inactiveFlagSet = rscFlags.isSomeSet(storDriverAccCtx, Resource.Flags.INACTIVE);
        boolean forceCreateMetaData = rscFlags.isSet(storDriverAccCtx, Resource.Flags.RESTORE_FROM_SNAPSHOT) ||
            rscData.getAbsResource().getResourceDefinition().getFlags()
                .isSet(storDriverAccCtx, ResourceDefinition.Flags.CLONING);

        for (BCacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
        {
            boolean vlmDeleteFlagSet = ((Volume) vlmData.getVolume()).getFlags()
                .isSomeSet(
                    storDriverAccCtx,
                    Volume.Flags.DELETE,
                    Volume.Flags.CLONING
            );
            boolean vlmDfnDeleteFlagSet = vlmData.getVolume().getVolumeDefinition().getFlags().isSet(
                storDriverAccCtx,
                VolumeDefinition.Flags.DELETE
            );
            if (deleteFlagSet || inactiveFlagSet || vlmDeleteFlagSet || vlmDfnDeleteFlagSet)
            {
                if (vlmData.exists())
                {
                    errorReporter.logDebug(
                        "BCache: removing %s (%s)",
                        vlmData.getIdentifier(),
                        vlmData.getDevicePath()
                    );
                    BCacheUtils.remove(errorReporter, vlmData.getDeviceUuid(), vlmData.getIdentifier());

                    if (deleteFlagSet)
                    {
                        wipeHandler.quickWipe(vlmData.getDataDevice());
                        wipeHandler.quickWipe(vlmData.getCacheDevice());
                        vlmData.setIdentifier(null);
                    }
                    vlmData.setExists(false);
                    vlmData.setDevicePath(null);
                }
                else
                {
                    errorReporter.logDebug(
                        "BCache: noop as no bcache device exist for %s",
                        toString(vlmData)
                    );
                }
            }
        }

        resourceProcessorProvider.get().process(
            rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA),
            snapshotListRef,
            apiCallRcRef
        );
        AbsRscLayerObject<Resource> cacheRscChild = rscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_BCACHE_CACHE);
        if (cacheRscChild == null)
        {
            // we might be an imaginary layer above an NVMe target which does not need a cache...
            errorReporter.logDebug("BCache: no devices provided to upper layer");
        }
        else
        {
            resourceProcessorProvider.get().process(
                cacheRscChild,
                snapshotListRef,
                apiCallRcRef
            );
        }

        if (!deleteFlagSet && !inactiveFlagSet)
        {
            for (BCacheVlmData<Resource> vlmData : rscData.getVlmLayerObjects().values())
            {
                boolean vlmDeleteFlagSet = ((Volume) vlmData.getVolume()).getFlags()
                    .isSomeSet(
                        storDriverAccCtx,
                        Volume.Flags.DELETE,
                        Volume.Flags.CLONING
                );
                boolean vlmDfnDeleteFlagSet = vlmData.getVolume().getVolumeDefinition().getFlags().isSet(
                    storDriverAccCtx,
                    VolumeDefinition.Flags.DELETE
                );
                if (!vlmDeleteFlagSet && !vlmDfnDeleteFlagSet)
                {

                    PriorityProps prioProps = getPrioProps(vlmData.getVolume());
                    if (!vlmData.exists())
                    {
                        VlmProviderObject<Resource> dataChild = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
                        VlmProviderObject<Resource> cacheChild = vlmData.getChildBySuffix(
                            RscLayerSuffixes.SUFFIX_BCACHE_CACHE
                        );

                        String backingDev = dataChild.getDevicePath();
                        String cacheDev = cacheChild.getDevicePath();

                        vlmData.setDataDevice(backingDev);
                        vlmData.setCacheDevice(cacheDev);

                        ArrayList<String> options = getMakeBCacheOptions(prioProps);

                        /*
                         * check first if the two devices were not just inactive, i.e. already contain bcache headers +
                         * data
                         */

                        UUID bcacheUuid;
                        String identifier;

                        UUID backingUuid = BCacheUtils.getCSetUuidFromSuperBlock(extCmdFactory, backingDev);
                        if (backingUuid == null || forceCreateMetaData)
                        {
                            if (forceCreateMetaData)
                            {
                                WipeHandler.wipeFs(extCmdFactory, backingDev);
                            }
                            backingUuid = BCacheUtils.makeBCache(extCmdFactory, backingDev, null, options);
                            identifier = waitUntilBackingDeviceIsRegistered(
                                backingDev,
                                WAIT_FOR_BCACHE_TIMEOUT_MS,
                                WAIT_FOR_BCACHE_RETRY_COUNT,
                                false
                            );
                            if (identifier == null)
                            {
                                BCacheUtils.register(errorReporter, backingDev);
                                identifier = waitUntilBackingDeviceIsRegistered(backingDev);
                            }
                        }
                        else
                        {
                            // basically checks if backing device is registered
                            identifier = BCacheUtils.getIdentifierByBackingUuid(
                                extCmdFactory,
                                BCacheUtils.getBackingId(extCmdFactory, backingDev)
                            );
                            if (identifier == null)
                            {
                                // not registered yet
                                BCacheUtils.register(errorReporter, backingDev);
                                identifier = waitUntilBackingDeviceIsRegistered(backingDev);
                            }
                        }

                        UUID cacheUuid = BCacheUtils.getCSetUuidFromSuperBlock(extCmdFactory, cacheDev);
                        if (cacheUuid == null || forceCreateMetaData)
                        {
                            if (forceCreateMetaData)
                            {
                                WipeHandler.wipeFs(extCmdFactory, cacheDev);
                            }
                            cacheUuid = BCacheUtils.makeBCache(extCmdFactory, null, cacheDev, options);
                        }
                        if (!isCacheDeviceRegistered(cacheUuid))
                        {
                            try
                            {
                                BCacheUtils.register(errorReporter, cacheDev);
                            }
                            catch (StorageException storExc)
                            {
                                /*
                                 * after creating a cache device and there is a small timeframe where the device might
                                 * have been registered already, but is not shown in the 'isCacheDeviceRegistered',
                                 * which makes Linstor to try to register it, but that might fail due to the race
                                 * condition
                                 */
                                if (!isCacheDeviceRegistered(cacheUuid))
                                {
                                    throw storExc;
                                }
                            }
                            waitForCacheDevice(cacheUuid);
                        }

                        if (!backingUuid.equals(cacheUuid))
                        {
                            // not attached
                            identifier = BCacheUtils.getIdentifierByBackingUuid(
                                extCmdFactory,
                                BCacheUtils.getBackingId(extCmdFactory, backingDev)
                            );
                            BCacheUtils.attach(errorReporter, cacheUuid, identifier);
                        }

                        // identifier = getIdentifier(cacheUuid);
                        bcacheUuid = cacheUuid;

                        vlmData.setDeviceUuid(bcacheUuid);
                        vlmData.setExists(true);
                        vlmData.setIdentifier(identifier);
                        vlmData.setDevicePath(buildDevicePath(vlmData.getIdentifier()));
                        errorReporter.logDebug("BCache: device (%s) created", vlmData.getDevicePath());
                    }

                    if (forceCreateMetaData)
                    {
                        // just in case something goes wrong later and we need to re-create the bcache metadata once
                        // more
                        vlmData.setDevicePath(null);
                    }
                }
            }
        }
    }


    private void waitForCacheDevice(UUID cacheUuidRef) throws StorageException
    {
        Path path = BCacheUtils.PATH_SYS_FS_BCACHE.resolve(cacheUuidRef.toString());
        boolean found = false;
        for (int retry = 0; retry < WAIT_FOR_BCACHE_RETRY_COUNT; retry++)
        {
            try
            {
                Thread.sleep(WAIT_FOR_BCACHE_TIMEOUT_MS);
                if (Files.exists(path))
                {
                    found = true;
                    break;
                }
            }
            catch (InterruptedException ignored)
            {
            }
        }
        if (!found)
        {
            throw new StorageException(
                "Cache device not showed up as " + path + " within " +
                    WAIT_FOR_BCACHE_RETRY_COUNT * WAIT_FOR_BCACHE_TIMEOUT_MS + "ms"
            );
        }
    }

    private String waitUntilBackingDeviceIsRegistered(String backingDev) throws StorageException
    {
        return waitUntilBackingDeviceIsRegistered(
            backingDev,
            WAIT_FOR_BCACHE_TIMEOUT_MS,
            WAIT_FOR_BCACHE_RETRY_COUNT,
            true
        );
    }

    private String waitUntilBackingDeviceIsRegistered(
        String backingDev,
        long timeout,
        long waitCount,
        boolean throwIfNotRegistered
    )
        throws StorageException
    {
        String identifier = null;
        UUID backingDevUuid = BCacheUtils.getBackingId(extCmdFactory, backingDev);
        for (int retry = 0; identifier == null && retry < waitCount; retry++)
        {
            try
            {
                Thread.sleep(timeout);
                identifier = BCacheUtils.getIdentifierByBackingUuid(extCmdFactory, backingDevUuid);
            }
            catch (InterruptedException ignored)
            {
            }
        }
        if (identifier == null && throwIfNotRegistered)
        {
            throw new StorageException(
                "Backing device " + backingDev + " was not registered within " +
                    waitCount * timeout + "ms"
            );
        }
        return identifier;

    }

    private boolean isCacheDeviceRegistered(UUID cacheUuidRef) throws StorageException
    {
        return Files.exists(BCacheUtils.PATH_SYS_FS_BCACHE.resolve(cacheUuidRef.toString()));
    }

    private ArrayList<String> getMakeBCacheOptions(PriorityProps prioProps)
    {
        Map<String, String> optionsMap = prioProps.renderRelativeMap(
            ApiConsts.NAMESPC_BCACHE
        );

        ArrayList<String> options = new ArrayList<>();
        for (Entry<String, String> entry : optionsMap.entrySet())
        {
            switch (entry.getKey())
            {
                case ApiConsts.KEY_BCACHE_BLOCKSIZE:
                    options.add("--block");
                    options.add(entry.getValue());
                    break;
                case ApiConsts.KEY_BCACHE_BUCKETSIZE:
                    options.add("--bucket");
                    options.add(entry.getValue());
                    break;
                case ApiConsts.KEY_BCACHE_DATA_OFFSET:
                    options.add("--data-offset");
                    options.add(entry.getValue());
                    break;
                case ApiConsts.KEY_BCACHE_WRITEBACK:
                    if (ApiConsts.VAL_TRUE.equalsIgnoreCase(entry.getValue()))
                    {
                        options.add("--writeback");
                    }
                    break;
                case ApiConsts.KEY_BCACHE_DISCARD:
                    if (ApiConsts.VAL_TRUE.equalsIgnoreCase(entry.getValue()))
                    {
                        options.add("--discard");
                    }
                    break;
                case ApiConsts.KEY_BCACHE_CACHE_REPLACEMENT_POLICY:
                    options.add("--cache_replcement_policy");
                    options.add(entry.getValue());
                    break;
                case ApiConsts.KEY_BCACHE_POOL_NAME:
                    // silently ignored
                    break;
                default:
                    errorReporter.logWarning(
                        "BCache: Ignoring unknown property: %s with value: %s",
                        entry.getKey(),
                        entry.getValue()
                    );
                    break;
            }
        }
        return options;
    }

    private String buildDevicePath(String identifierRef)
    {
        return "/dev/" + identifierRef;
    }

    @Override
    public void clearCache() throws StorageException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(Props localNodePropsRef)
        throws StorageException, AccessDeniedException
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
            AbsRscLayerObject<Resource> cacheRscChild = layerDataRef
                .getChildBySuffix(RscLayerSuffixes.SUFFIX_WRITECACHE_CACHE);
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

    private String toString(BCacheVlmData<Resource> vlmData)
    {
        StringBuilder sb = new StringBuilder("Resource: ")
            .append(vlmData.getRscLayerObject().getResourceName().displayValue)
            .append(" Volume number: ").append(vlmData.getVlmNr().value);
        String rscNameSuffix = vlmData.getRscLayerObject().getResourceNameSuffix();
        if (rscNameSuffix != null && !rscNameSuffix.equals(RscLayerSuffixes.SUFFIX_DATA))
        {
            sb.append(" Suffix: ").append(rscNameSuffix);
        }
        return sb.toString();
    }

    private String getCacheSize(AbsVolume<Resource> vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getPrioProps(vlmRef).getProp(
            ApiConsts.KEY_BCACHE_SIZE,
            ApiConsts.NAMESPC_BCACHE,
            DFLT_CACHE_SIZE
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
