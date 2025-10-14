package com.linbit.linstor.layer.dmsetup.cache;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ChildProcessHandler.TimeoutType;
import com.linbit.extproc.ExtCmd;
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
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.Commands;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CacheLayer implements DeviceLayer
{
    private static final String FORMAT_DM_NAME = "linstor_cache_%s%s_%05d";
    private static final String FORMAT_DEV_PATH = "/dev/mapper/%s";

    private static final String DFLT_FEATURE = "writeback";
    private static final String DFLT_POLICY = "smq";
    private static final long DFLT_FLUSH_TIMEOUT_IN_MS = 3 * 60_000; // 3 minutes
    private static final long DFLT_DMSETUP_WAIT_TIMEOUT_IN_MS = 10_000;

    private final ErrorReporter errorReporter;
    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;

    private @Nullable Props localNodeProps;
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

    /**
     * Suspending a dm-cache device is relatively easy. Forcing it to also flush the data to its origin (backing)
     * device is quite tricky.
     * The general procedure looks as follows:
     * <ul>
     *  <li><code>dmsetup reload $cache_dev --table ...</code> using <code>cleaner</code> policy. This should flush
     *      the content to the origin device eventually, but it might take about 20-30 seconds. Since we do not want
     *      to wait for that long, we will also need to play around with the <code>migration_threashold</code> to
     *      speed things up.</li>
     *  <li><code>dmsetup resume $cache_dev</code>. This is necessary for the just set <code>cleaner</code> policy to
     *      get applied/activated</li>
     *  <li><code>dmsetup message $cache_dev 0 migration_threshold $nr_of_sectory</code> to speed up migrating data
     *      from the cache device to the origin</li>
     *  <li><code>dmsetup suspend $cache_dev</code>. We will still need to wait for all dirty blocks to get written,
     *      but we at least want the application above us to stop writing new data.</li>
     *  <li>Wait until <code>dmsetup status $cache_dev</code> shows 0 dirty blocks. We want to avoid a busy loop which
     *      would rapid fire <code>dmsetup status</code> commands. Instead we hope for
     *      <code>dmsetup wait $cache_dev $last_known_event_nr</code> to wake us up in dirty blocks got migrated to
     *      the origin device.</li>
     * </ul>
     *
     * This method will block until all dirty blocks are written to the origin (backing) device.
     * @throws IOException
     * @throws ChildProcessTimeoutException
     * @throws AccessDeniedException
     */
    @Override
    public void suspendIo(AbsRscLayerObject<Resource> rscDataRef, boolean ignoredAsRootLayerRef)
        throws ExtCmdFailedException, StorageException, ChildProcessTimeoutException, IOException, AccessDeniedException
    {
        // the general procedure is described in the java-doc, but we still try to change the policy and send the
        // 'migration_threshold' message to all volumes asap and only afterwards wait for all volumes to finish
        // with the migration, hence the two loops over the same vlmData
        Collection<? extends VlmProviderObject<Resource>> vlmDataCol = rscDataRef.getVlmLayerObjects().values();
        for (VlmProviderObject<Resource> vlmData : vlmDataCol)
        {
            CacheVlmData<Resource> cacheVlmData = (CacheVlmData<Resource>) vlmData;
            // DmSetupUtils.suspendIo(errorReporter, extCmdFactory, cacheVlmData, true, null);4
            long endSector = Commands.getDeviceSizeInSectors(extCmdFactory.create(), cacheVlmData.getDataDevice());
            @Nullable String cacheDevIdentifier = cacheVlmData.getIdentifier();
            if (cacheDevIdentifier == null)
            {
                throw new StorageException(
                    "Cache device has unexpectedly no identifier. " + cacheVlmData.getDevicePath() + ", " + cacheVlmData
                        .getDataDevice()
                );
            }
            DmSetupUtils.reload(
                extCmdFactory,
                cacheDevIdentifier,
                getDmSetupTable(cacheVlmData, OverridePolicy.CLEANER, endSector)
            );
            // for some reason "cleaner" policy only starts working when we also resume-io...
            DmSetupUtils.suspendIo(errorReporter, extCmdFactory, cacheVlmData, false, null);
            DmSetupUtils.message(
                extCmdFactory,
                cacheDevIdentifier,
                0L,
                new String[]
                {
                    "migration_threshold", Long.toString(endSector)
                }
            );

            long start = System.currentTimeMillis();
            long now = start;
            long dirtyCacheBlocks = 1;
            long flushTimeout = getFlushTimeout(cacheVlmData);
            while (dirtyCacheBlocks > 0 && now - start < flushTimeout)
            {
                long lastEventNr = DmSetupUtils.getLastEventNr(extCmdFactory, cacheVlmData);
                dirtyCacheBlocks = DmSetupUtils.getDirtyCacheBlocks(extCmdFactory, cacheDevIdentifier);
                if (dirtyCacheBlocks > 0)
                {
                    ExtCmd extCmd = extCmdFactory.create();
                    extCmd.setTimeout(TimeoutType.WAIT, DFLT_DMSETUP_WAIT_TIMEOUT_IN_MS);
                    try
                    {
                        DmSetupUtils.wait(extCmd, cacheDevIdentifier, lastEventNr);
                    }
                    catch (ChildProcessTimeoutException ignored)
                    {
                    }
                }

                now = System.currentTimeMillis();
            }

            if (dirtyCacheBlocks > 0)
            {
                throw new StorageException(
                    "Cache device " + cacheDevIdentifier + " did not flush its data within " + flushTimeout + "ms"
                );
            }
            DmSetupUtils.suspendIo(errorReporter, extCmdFactory, cacheVlmData, true, null);
        }
    }

    private long getFlushTimeout(CacheVlmData<Resource> cacheVlmDataRef) throws AccessDeniedException
    {
        long ret;
        @Nullable String val = getPrioProps(cacheVlmDataRef.getVolume()).getProp(
            ApiConsts.KEY_CACHE_FLUSH_TIMEOUT,
            ApiConsts.NAMESPC_CACHE
        );
        if (val == null)
        {
            ret = DFLT_FLUSH_TIMEOUT_IN_MS;
        }
        else
        {
            ret = Long.parseLong(val);
        }
        return ret;
    }

    private String getDmSetupTable(CacheVlmData<Resource> cacheVlmDataRef) throws StorageException
    {
        return getDmSetupTable(cacheVlmDataRef, OverridePolicy.DEFAULT, null);
    }

    private String getDmSetupTable(
        CacheVlmData<Resource> cacheVlmDataRef,
        OverridePolicy overridePolicyRef,
        @Nullable Long endSectorRef
    )
        throws StorageException
    {
        try
        {
            // TODO: properly use dmsetupPolicyArgsMap
            // ResourceDefinition rscDfn = vlm.getResourceDefinition();
            // ResourceGroup rscGrp = rscDfn.getResourceGroup();
            // PriorityProps prioProps = new PriorityProps(
            // vlm.getProps(storDriverAccCtx),
            // vlm.getVolumeDefinition().getProps(storDriverAccCtx),
            // rscDfn.getProps(storDriverAccCtx),
            // rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmData.getVlmNr()),
            // rscGrp.getProps(storDriverAccCtx),
            // localNodeProps,
            // stltConfAccessor.getReadonlyProps()
            // );
            // Map<String, String> dmsetupPolicyArgsMap = prioProps.renderRelativeMap(
            // ApiConsts.NAMESPC_CACHE_POLICY_ARGS
            // );
            AbsVolume<Resource> vlm = cacheVlmDataRef.getVolume();
            String dataDevice = Objects.requireNonNull(cacheVlmDataRef.getDataDevice());

            @Nullable String feature;
            String policy;
            switch (overridePolicyRef)
            {
                case DEFAULT:
                    feature = getFeature(vlm);
                    policy = getPolicy(vlm);
                    break;
                case CLEANER:
                    feature = null;
                    policy = "cleaner";
                    break;
                default:
                    throw new ImplementationError("unexpected overridePolicy:" + overridePolicyRef);
            }
            return DmSetupUtils.getCacheTable(
                0L,
                endSectorRef == null ?
                    Commands.getDeviceSizeInSectors(extCmdFactory.create(), dataDevice) :
                    endSectorRef,
                dataDevice,
                Objects.requireNonNull(cacheVlmDataRef.getCacheDevice()),
                Objects.requireNonNull(cacheVlmDataRef.getMetaDevice()),
                getBlocksize(vlm),
                feature,
                policy
            );
        }
        catch (NullPointerException npe)
        {
            throw new StorageException(
                "Failed to create dmsetup table for cache device for volume:" +
                    cacheVlmDataRef.getRscLayerObject().getSuffixedResourceName() + "/" + cacheVlmDataRef.getVlmNr(),
                npe
            );
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void resumeIo(AbsRscLayerObject<Resource> rscDataRef, boolean ignoredAsRootLayerRef)
        throws ExtCmdFailedException, StorageException
    {
        for (VlmProviderObject<Resource> vlmData : rscDataRef.getVlmLayerObjects().values())
        {
            CacheVlmData<Resource> cacheVlmData = (CacheVlmData<Resource>) vlmData;
            DmSetupUtils.reload(
                extCmdFactory,
                cacheVlmData.getIdentifier(),
                getDmSetupTable(cacheVlmData, OverridePolicy.DEFAULT, null)
            );
            DmSetupUtils.suspendIo(errorReporter, extCmdFactory, vlmData, false, null);
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
                    if (!vlmData.exists())
                    {
                        vlmData.setDataDevice(
                            vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA).getDevicePath()
                        );
                        vlmData.setCacheDevice(
                            vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_CACHE).getDevicePath()
                        );
                        vlmData.setMetaDevice(
                            vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_CACHE_META).getDevicePath()
                        );

                        DmSetupUtils.create(extCmdFactory, vlmData.getIdentifier(), getDmSetupTable(vlmData));

                        vlmData.setDevicePath(String.format(FORMAT_DEV_PATH, vlmData.getIdentifier()));
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

    @Override
    public DeviceLayerKind getKind()
    {
        return DeviceLayerKind.CACHE;
    }

    private enum OverridePolicy
    {
        DEFAULT,
        CLEANER
    }
}
