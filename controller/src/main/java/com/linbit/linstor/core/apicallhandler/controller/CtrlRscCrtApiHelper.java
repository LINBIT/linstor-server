package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiConsts.ConnectionStatus;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.compat.CompatibilityUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.AllocationGranularityHelper;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceCreateCheck;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceControllerFactory;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.layer.storage.BlockSizeConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.linstor.utils.layer.DrbdLayerUtils;
import com.linbit.linstor.utils.layer.LayerKindUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.MathUtils;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.api.ApiConsts.MASK_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.MASK_WARN;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Singleton
public class CtrlRscCrtApiHelper
{
    private static final long DFLT_RSC_READY_WAIT_TIME_IN_MS = 15_000;

    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final EventWaiter eventWaiter;
    private final ResourceStateEvent resourceStateEvent;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceControllerFactory resourceFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final ResourceCreateCheck resourceCreateCheck;
    private final CtrlRscLayerDataFactory layerDataHelper;
    private final Provider<CtrlRscAutoHelper> autoHelper;
    private final Provider<CtrlRscToggleDiskApiCallHandler> toggleDiskHelper;
    private final SharedResourceManager sharedRscMgr;
    private final BackupInfoManager backupInfoMgr;
    private final ScheduleBackupService scheduleBackupService;
    private final CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandler;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandler;
    private final AllocationGranularityHelper allocationGranularityHelper;
    private final NodeRepository nodeRepository;
    private final CtrlRscStateHelper rscStateHelper;
    private final CtrlMinIoSizeHelper minIoSizeHelper;
    private final ResourceDefinitionRepository rscDfnRepo;

    @Inject
    CtrlRscCrtApiHelper(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        EventWaiter eventWaiterRef,
        ResourceStateEvent resourceStateEventRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceControllerFactory resourceFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ResourceCreateCheck resourceCreateCheckRef,
        CtrlRscLayerDataFactory layerDataHelperRef,
        Provider<CtrlRscAutoHelper> autoHelperRef,
        Provider<CtrlRscToggleDiskApiCallHandler> toggleDiskHelperRef,
        SharedResourceManager sharedRscMgrRef,
        BackupInfoManager backupInfoMgrRef,
        ScheduleBackupService scheduleBackupServiceRef,
        CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandlerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerRef,
        AllocationGranularityHelper allocationGranularityHelperRef,
        NodeRepository nodeRepositoryRef,
        CtrlRscStateHelper rscStateHelperRef,
        CtrlMinIoSizeHelper minIoSizeHelperRef,
        ResourceDefinitionRepository rscDfnRepoRef
    )
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        eventWaiter = eventWaiterRef;
        resourceStateEvent = resourceStateEventRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceFactory = resourceFactoryRef;
        peerAccCtx = peerAccCtxRef;
        resourceCreateCheck = resourceCreateCheckRef;
        layerDataHelper = layerDataHelperRef;
        autoHelper = autoHelperRef;
        toggleDiskHelper = toggleDiskHelperRef;
        sharedRscMgr = sharedRscMgrRef;
        backupInfoMgr = backupInfoMgrRef;
        scheduleBackupService = scheduleBackupServiceRef;
        ctrlRscActivateApiCallHandler = ctrlRscActivateApiCallHandlerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        allocationGranularityHelper = allocationGranularityHelperRef;
        nodeRepository = nodeRepositoryRef;
        rscStateHelper = rscStateHelperRef;
        rscDfnRepo = rscDfnRepoRef;
        minIoSizeHelper = minIoSizeHelperRef;
    }

    /**
     * This method really creates the resource and its volumes.
     *
     * This method does NOT:
     * * commit any transaction
     * * update satellites
     * * create success-apiCallRc entries (only error RC in case of exception)
     *
     * @return the newly created resource
     */
    public PairNonNull<List<Flux<ApiCallRc>>, ApiCallRcWith<Resource>> createResourceDb(
        String nodeNameStr,
        String rscNameStr,
        long flags,
        Map<String, String> rscPropsMap,
        List<? extends VolumeApi> vlmApiList,
        @Nullable Integer nodeIdInt,
        @Nullable List<Integer> portsRef,
        @Nullable Integer portCountRef,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities,
        List<String> layerStackStrListRef,
        @Nullable Resource.DiskfulBy diskfulByRef
    )
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

        boolean canChangeMinIo = false;
        try
        {
            if (rscDfn != null)
            {
                if (rscDfn.getResourceCount() >= 1)
                {
                    canChangeMinIo = rscStateHelper.canChangeMinIoSize(rscDfn);
                }
                else
                {
                    // No currently deployed resources
                    canChangeMinIo = minIoSizeHelper.isAutoMinIoSize(rscDfn, apiCtx);
                }
            }
            else
            {
                throw new ImplementationError(
                    "createResourceDb with rscDfn == null in " +
                    CtrlRscCrtApiHelper.class.getSimpleName()
                );
            }
        }
        catch (AccessDeniedException accExc)
        {
            // FIXME: ImplementationError rather?
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN,
                    "Cannot access the resource definition of the resource to deploy"
                )
            );
        }

        long adjustedFlags = flags;
        List<Flux<ApiCallRc>> autoFlux = new ArrayList<>();
        Resource rsc;
        ApiCallRcImpl responses = new ApiCallRcImpl();

        AccessContext peerCtx = peerAccCtx.get();
        Node node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
        if (backupInfoMgr.restoreContainsRscDfn(rscDfn))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    rscNameStr + " is currently being restored from a backup. " +
                        "Please wait until the restore is finished"
                )
            );
        }
        if (isNodeFlagSet(node, Node.Flags.EVACUATE))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EVACUATING,
                    "node '" + nodeNameStr + " is being evacuated. No new resources allowed on this node."
                )
            );
        }

        Resource rscForToggleDiskful = ctrlApiDataLoader.loadRsc(node.getName(), rscDfn.getName(), false);
        if (rscForToggleDiskful != null && !isFlagSet(rscForToggleDiskful, Resource.Flags.DRBD_DISKLESS))
        {
            // diskful resource, do not try to toggle this
            rscForToggleDiskful = null;
        }

        String storPoolName = rscPropsMap.get(ApiConsts.KEY_STOR_POOL_NAME);
        if (rscForToggleDiskful != null)
        {
            rsc = rscForToggleDiskful;
            autoHelper.get().removeTiebreakerFlag(rscForToggleDiskful); // just in case this was a tiebreaker
            String storPoolNameStr = storPoolName;
            StorPool storPool = storPoolNameStr == null ?
                null : ctrlApiDataLoader.loadStorPool(storPoolNameStr, nodeNameStr, false);

            boolean isDiskless = FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.DISKLESS) || // needed for
                                                                                              // compatibility
                FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.DRBD_DISKLESS) ||
                FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.NVME_INITIATOR) ||
                FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.EBS_INITIATOR) ||
                (storPool != null && storPool.getDeviceProviderKind().equals(DeviceProviderKind.DISKLESS));

            if (FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.INACTIVE))
            {
                autoFlux.add(ctrlRscActivateApiCallHandler.deactivateRsc(nodeNameStr, rscNameStr));
            }

            if (!isDiskless)
            {
                // target resource is diskful
                autoFlux.add(
                    toggleDiskHelper.get().resourceToggleDisk(
                        nodeNameStr,
                        rscNameStr,
                        storPoolNameStr,
                        null,
                        layerStackStrListRef,
                        false,
                        diskfulByRef
                    )
                );
            }
            else
            {
                if (isFlagSet(rscForToggleDiskful, Resource.Flags.TIE_BREAKER))
                {
                    // target resource is diskless.
                    NodeName tiebreakerNodeName = rscForToggleDiskful.getNode().getName();
                    autoFlux.add(
                        ctrlSatelliteUpdateCaller.updateSatellites(
                            rscForToggleDiskful.getResourceDefinition(),
                            Flux.empty() // if failed, there is no need for the retry-task to wait for readyState
                            // this is only true as long as there is no other flux concatenated after readyResponses
                        )
                            .transform(
                                updateResponses -> CtrlResponseUtils.combineResponses(
                                    errorReporter,
                                    updateResponses,
                                    rscDfn.getName(),
                                    Collections.singleton(tiebreakerNodeName),
                                    "Removed TIE_BREAKER flag from resource {1} on {0}",
                                    "Update of resource {1} on '" + tiebreakerNodeName + "' applied on node {0}"
                                )
                            )
                    );
                }
                else
                {
                    // noop, resource is already diskless as expected
                }
            }
        }
        else
        {
            LayerPayload payload = new LayerPayload();
            DrbdRscPayload drbdRsc = payload.getDrbdRsc();
            drbdRsc.nodeId = nodeIdInt;
            drbdRsc.tcpPorts = portsRef == null ? null : new TreeSet<>(portsRef);
            drbdRsc.portCount = portCountRef;
            if (storPoolName != null)
            {
                // null if resource is created with "-d" (diskless)
                StorPool storPool = ctrlApiDataLoader.loadStorPool(storPoolName, nodeNameStr, true);

                Iterator<VolumeDefinition> vlmDfnIt = getVlmDfnIterator(rscDfn);
                while (vlmDfnIt.hasNext())
                {
                    /*
                     * We need to add this to avoid a chicken-egg problem:
                     *
                     * after creating the resource-instance, the RscFactory also creates all necessary layer-objects
                     * the DRBD-layer-object needs to know whether or not the resource is placed within a shared storage
                     * pool
                     * however, we cannot ask the volumes of the resource, as those do not exist at the given time.
                     * the resource-properties are also only copied into the resource-instance after its creation (duh)
                     */
                    VolumeDefinition vlmDfn = vlmDfnIt.next();
                    payload.putStorageVlmPayload(
                        RscLayerSuffixes.SUFFIX_DATA,
                        vlmDfn.getVolumeNumber().value,
                        storPool
                    );
                }
            }

            List<DeviceLayerKind> layerStack = getLayerstackOrBuildDefault(
                peerCtx,
                layerDataHelper,
                errorReporter,
                layerStackStrListRef,
                responses,
                rscDfn
            );

            // compatibility
            String storPoolNameStr = storPoolName;
            StorPool storPool = storPoolNameStr == null ?
                null :
                ctrlApiDataLoader.loadStorPool(
                    storPoolNameStr,
                    nodeNameStr,
                    false
                );

            boolean isStorPoolDiskless = false;
            if (storPool != null)
            {
                DeviceProviderKind devProviderKind = storPool.getDeviceProviderKind();
                isStorPoolDiskless = !devProviderKind.hasBackingDevice();
            }

            boolean isDisklessSet = FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.DISKLESS);
            boolean isDrbdDisklessSet = FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.DRBD_DISKLESS);
            boolean isNvmeInitiatorSet = FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.NVME_INITIATOR);
            boolean isEbsInitiatorSet = FlagsHelper.isFlagEnabled(adjustedFlags, Resource.Flags.EBS_INITIATOR);

            if (
                (isDisklessSet && !isDrbdDisklessSet && !isNvmeInitiatorSet && !isEbsInitiatorSet) ||
                (!isDisklessSet && isStorPoolDiskless)
            )
            {
                if (layerStack.isEmpty())
                {
                    adjustedFlags |= Resource.Flags.DRBD_DISKLESS.flagValue;
                    responses.addEntry(makeFlaggedDrbdDisklessWarning(storPool));
                }
                else
                {
                    Flags disklessNvmeOrDrbd = CompatibilityUtils.mapDisklessFlagToNvmeOrDrbd(layerStack);
                    if (storPool != null)
                    {
                        if (disklessNvmeOrDrbd.equals(Resource.Flags.DRBD_DISKLESS))
                        {
                            responses.addEntry(makeFlaggedDrbdDisklessWarning(storPool));
                        }
                        else
                        {
                            responses.addEntry(makeFlaggedNvmeInitiatorWarning(storPool));
                        }
                    }
                    adjustedFlags |= disklessNvmeOrDrbd.flagValue;

                    if (
                        FlagsHelper.isFlagEnabled(
                            adjustedFlags,
                            Resource.Flags.DRBD_DISKLESS,
                            Resource.Flags.NVME_INITIATOR
                        )
                    )
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_LAYER_STACK,
                                "Could not figure out how to interpret the deprecated --diskless flag."
                            )
                                .setDetails(
                                    "The general DISKLESS flag is deprecated. If both layers, DRBD and NVME, should " +
                                        "be used LINSTOR has to figure out if the resource should be diskful for " +
                                        "DRBD (required NVME_INITIATOR) or diskless for DRBD " +
                                        "(requires DRBD_DISKLESS). Using the deprecated DISKLESS flag is not " +
                                        "supported for this case."
                                )
                                .setCorrection("Use either a non-deprecated flag or do not use both layers")
                        );
                    }
                }
            }

            rsc = createResource(rscDfn, node, payload, adjustedFlags, layerStack);
            Props rscProps = ctrlPropsHelper.getProps(rsc);

            ctrlPropsHelper.fillProperties(
                responses, LinStorObject.RSC, rscPropsMap, rscProps, ApiConsts.FAIL_ACC_DENIED_RSC
            );

            if (ctrlVlmCrtApiHelper.isDiskless(rsc) && storPoolNameStr == null)
            {
                rscProps.map().put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);
            }

            for (VolumeApi vlmApi : vlmApiList)
            {
                VolumeDefinition vlmDfn = loadVlmDfn(rscDfn, vlmApi.getVlmNr(), true);

                Volume vlmData = ctrlVlmCrtApiHelper.createVolumeResolvingStorPool(
                    rsc,
                    vlmDfn,
                    thinFreeCapacities,
                    Collections.emptyMap()
                ).extractApiCallRc(responses);

                Props vlmProps = ctrlPropsHelper.getProps(vlmData);

                ctrlPropsHelper.fillProperties(
                    responses, LinStorObject.VLM, vlmApi.getVlmProps(), vlmProps, ApiConsts.FAIL_ACC_DENIED_VLM
                );
            }

            Iterator<VolumeDefinition> iterateVolumeDfn = getVlmDfnIterator(rscDfn);
            while (iterateVolumeDfn.hasNext())
            {
                VolumeDefinition vlmDfn = iterateVolumeDfn.next();

                // first check if we probably just deployed a vlm for this vlmDfn
                if (rsc.getVolume(vlmDfn.getVolumeNumber()) == null)
                {
                    // not deployed yet.

                    Volume vlm = ctrlVlmCrtApiHelper.createVolumeResolvingStorPool(
                        rsc,
                        vlmDfn,
                        thinFreeCapacities,
                        Collections.emptyMap()
                    ).extractApiCallRc(responses);

                    setDrbdPropsForThinVolumesIfNeeded(vlm);
                }
            }

            rscMinIoSizeCheck(rsc, layerStack, responses, canChangeMinIo);
            resourceCreateCheck.checkCreatedResource(rsc);
        }

        if (isFlagSet(rsc, Resource.Flags.DELETE) || isFlagSet(rsc, Resource.Flags.DRBD_DELETE))
        {
            disableFlags(rsc, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE);
            rsc.streamVolumes()
                .forEach(
                    vlm -> disableFlags(vlm, Volume.Flags.DELETE, Volume.Flags.DRBD_DELETE)
                );

            ResourceDataUtils.recalculateVolatileRscData(layerDataHelper, rsc);
        }

        if (!isFlagSet(rsc, Resource.Flags.INACTIVE) && !sharedRscMgr.isActivationAllowed(rsc))
        {
            autoFlux.add(ctrlRscActivateApiCallHandler.deactivateRsc(nodeNameStr, rscNameStr));
        }

        return new PairNonNull<>(autoFlux, new ApiCallRcWith<>(responses, rsc));
    }

    private void rscMinIoSizeCheck(
        final Resource rsc,
        final List<DeviceLayerKind> layerStack,
        final ApiCallRcImpl responses,
        final boolean canChangeMinIo
    )
    {
        final Iterator<VolumeDefinition> vlmDfnIter;
        try
        {
            final ResourceDefinition rscDfn = rsc.getResourceDefinition();
            vlmDfnIter = rscDfn.iterateVolumeDfn(apiCtx);

            boolean hasSpecialLayers = LayerKindUtils.hasSpecialLayers(layerStack);
            boolean haveChangedMinIo = false;
            while (vlmDfnIter.hasNext())
            {
                final VolumeDefinition vlmDfn = vlmDfnIter.next();
                final Props vlmDfnProps = vlmDfn.getProps(apiCtx);
                final @Nullable String vlmBlockSizeStr = vlmDfnProps.getProp(
                    InternalApiConsts.KEY_DRBD_BLOCK_SIZE,
                    ApiConsts.NAMESPC_DRBD_DISK_OPTIONS
                );
                long vlmBlockSize = BlockSizeConsts.DFLT_IO_SIZE;
                if (vlmBlockSizeStr != null)
                {
                    try
                    {
                        final long value = Long.parseLong(vlmBlockSizeStr);
                        vlmBlockSize = MathUtils.bounds(
                            BlockSizeConsts.MIN_IO_SIZE,
                            value,
                            BlockSizeConsts.MAX_IO_SIZE
                        );
                    }
                    catch (NumberFormatException ignored)
                    {
                        errorReporter.logWarning(
                            "Could not parse minIoSize '%s' of '%s', defaulting to %d", vlmBlockSizeStr,
                            vlmDfn.toStringImpl(), vlmBlockSize
                        );
                    }
                }

                long poolBlockSize = hasSpecialLayers ?
                    BlockSizeConsts.DFLT_SPECIAL_IO_SIZE : BlockSizeConsts.DFLT_IO_SIZE;

                if (!hasSpecialLayers)
                {
                    Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, apiCtx, false);
                    for (StorPool storPool : storPools)
                    {
                        final Props poolProps;
                        try
                        {
                            poolProps = storPool.getProps(apiCtx);
                        }
                        catch (AccessDeniedException accExc)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_ACC_DENIED_STOR_POOL, accExc)
                            );
                        }
                        final String poolBlockSizeStr = poolProps.getProp(
                            StorageConstants.BLK_DEV_MIN_IO_SIZE,
                            StorageConstants.NAMESPACE_INTERNAL
                        );
                        if (poolBlockSizeStr != null)
                        {
                            try
                            {
                                final long value = Long.parseLong(poolBlockSizeStr);
                                poolBlockSize = MathUtils.bounds(
                                    BlockSizeConsts.MIN_IO_SIZE,
                                    value,
                                    BlockSizeConsts.MAX_IO_SIZE
                                );
                            }
                            catch (NumberFormatException ignored)
                            {
                                errorReporter.logWarning(
                                    "Could not parse minIoSize '%s' of '%s', defaulting to %d",
                                    poolBlockSizeStr, storPool.toStringImpl(), poolBlockSize
                                );
                            }
                        }
                    }
                }

                final boolean autoMinIoSizeForVlmDfn = minIoSizeHelper.isAutoMinIoSize(vlmDfn, apiCtx);
                boolean minIoNeedsUpdate;
                if (vlmBlockSizeStr == null)
                {
                    // if the property was not set until now, but autoMinIoSize is enabled, we need to set
                    // the minio size property.
                    minIoNeedsUpdate = autoMinIoSizeForVlmDfn;
                }
                else
                {
                    minIoNeedsUpdate = autoMinIoSizeForVlmDfn && poolBlockSize > vlmBlockSize;
                }
                if (minIoNeedsUpdate)
                {
                    if (canChangeMinIo)
                    {
                        // Set the changed minIoSize
                        vlmDfn.setMinIoSize(poolBlockSize, apiCtx);
                        haveChangedMinIo = true;
                    }
                    else
                    {
                        // Cannot deploy if the target storage pool of any of the volumes has a larger
                        // minimum I/O size than what's currently set in the volume definition
                        throw new ApiRcException(
                            ApiCallRcImpl.entryBuilder(
                                ApiConsts.FAIL_INVLD_BLK_SIZE,
                                "Cannot create resource \"" + rscDfn.getName().displayValue + "\" on node \"" +
                                    rsc.getNode().getName().displayValue + "\", " +
                                    "storage pool has an incompatible minimum I/O size"
                            ).build()
                        );
                    }
                }
                else if (vlmBlockSizeStr == null)
                {
                    // initialize the minIoSize so that we have the property set. If we omit this step, the next time a
                    // satellite reconnects the code there would see the missing property and would set it (to the
                    // default 512) but would also set the DrbdRestart property, which is quite unnecessary.
                    vlmDfn.setMinIoSize(vlmBlockSize, apiCtx);
                }
            }
            if (haveChangedMinIo)
            {
                // Restart all DRBD resources if the minimum I/O size was changed
                rscDfn.requireDrbdRestart(apiCtx);
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(accExc);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    static List<DeviceLayerKind> getLayerstackOrBuildDefault(
        AccessContext accCtx,
        CtrlRscLayerDataFactory layerDataHelper,
        ErrorReporter errorReporter,
        List<String> layerStackStrListRef,
        ApiCallRcImpl responses,
        ResourceDefinition rscDfn
    )
    {
        List<DeviceLayerKind> layerStack = LinstorParsingUtils.asDeviceLayerKind(layerStackStrListRef);

        if (layerStack.isEmpty())
        {
            layerStack = getLayerStack(accCtx, rscDfn);
            if (layerStack.isEmpty())
            {
                Set<List<DeviceLayerKind>> existingLayerStacks = extractExistingLayerStacks(
                    accCtx,
                    layerDataHelper,
                    rscDfn
                );
                switch (existingLayerStacks.size())
                {
                    case 0: // ignore, will be filled later by CtrlLayerDataHelper#createDefaultLayerStack
                        // but that method requires the resource to already exist.
                        break;
                    case 1:
                        layerStack = existingLayerStacks.iterator().next();
                        break;
                    default:
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_LAYER_STACK,
                                "Could not figure out what layer-list to default to."
                            )
                                .setDetails(
                                    "Layer lists of already existing resources: \n   " +
                                        StringUtils.join(existingLayerStacks, "\n   ")
                                )
                                .setCorrection("Please specify a layer-list")
                        );

                }
            }
        }
        else
        {
            if (!layerStack.get(layerStack.size() - 1).equals(DeviceLayerKind.STORAGE))
            {
                layerStack.add(DeviceLayerKind.STORAGE);
                warnAddedStorageLayer(errorReporter, responses);
            }
        }
        return layerStack;
    }

    private static void warnAddedStorageLayer(ErrorReporter errorReporter, ApiCallRcImpl responsesRef)
    {
        String warnMsg = "The layerstack was extended with STORAGE kind.";
        errorReporter.logWarning(warnMsg);

        responsesRef.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.WARN_STORAGE_KIND_ADDED,
                warnMsg
            )
            .setDetails("Layer stacks have to be based on STORAGE kind. Layers configured to be diskless\n" +
                "will not use the additional STORAGE layer.")
            .build()
        );
    }

    private void setDrbdPropsForThinVolumesIfNeeded(Volume vlmRef)
    {
        try
        {
            AccessContext peerCtx = peerAccCtx.get();
            AbsRscLayerObject<Resource> rscLayerObj = vlmRef.getAbsResource().getLayerData(peerCtx);
            if (LayerUtils.hasLayer(rscLayerObj, DeviceLayerKind.DRBD))
            {
                boolean hasThinStorPool = false;
                boolean hasFatStorPool = false;
                // only set true on ZFS, see internal gitlab issue 671 for details
                boolean discardZerosIfAligned = false;

                List<AbsRscLayerObject<Resource>> storageRscLayerObjList = LayerUtils.getChildLayerDataByKind(
                    rscLayerObj,
                    DeviceLayerKind.STORAGE
                );
                for (AbsRscLayerObject<Resource> storageRsc : storageRscLayerObjList)
                {
                    if (RscLayerSuffixes.isNonMetaDataLayerSuffix(storageRsc.getResourceNameSuffix()))
                    {
                        for (VlmProviderObject<Resource> storageVlm : storageRsc.getVlmLayerObjects().values())
                        {
                            StorPool storPool = storageVlm.getStorPool();
                            DeviceProviderKind devProviderKind = storPool.getDeviceProviderKind();
                            switch (devProviderKind)
                            {
                                case DISKLESS: // ignored
                                    break;
                                case LVM: // fall-through
                                case SPDK: // fall-through
                                case REMOTE_SPDK: // fall-through
                                case EBS_INIT: // fall-through
                                case EBS_TARGET: // fall-through
                                case STORAGE_SPACES:
                                    hasFatStorPool = true;
                                    break;
                                case FILE:
                                    // TODO: introduce storage pool specific distinction about this
                                    hasFatStorPool = true;
                                    break;
                                case LVM_THIN:
                                    hasThinStorPool = true;
                                    discardZerosIfAligned = true;
                                    break;
                                case ZFS:
                                    hasFatStorPool = true;
                                    discardZerosIfAligned = true;
                                    break;
                                case ZFS_THIN:
                                    discardZerosIfAligned = true;
                                    // fall-through
                                case FILE_THIN:
                                case STORAGE_SPACES_THIN:
                                    hasThinStorPool = true;
                                    break;
                                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                                    // fall-through
                                default:
                                    throw new ImplementationError("Unknown deviceProviderKind: " + devProviderKind);
                            }
                        }
                    }
                }
                if (hasThinStorPool && hasFatStorPool)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_STOR_DRIVER,
                            "Mixing thin and thick storage pools are not allowed"
                        )
                    );
                }

                //TODO: make these default drbd-properties configurable (provider-specific?)

                ResourceDefinition rscDfn = vlmRef.getVolumeDefinition().getResourceDefinition();
                Props vlmDfnProps = vlmRef.getVolumeDefinition().getProps(peerCtx);
                PriorityProps prioProps = new PriorityProps(vlmDfnProps,
                    rscDfn.getProps(peerCtx),
                    rscDfn.getResourceGroup().getVolumeGroupProps(peerCtx, vlmRef.getVolumeNumber()),
                    rscDfn.getResourceGroup().getProps(peerCtx)
                );
                if (prioProps.getProp("discard-zeroes-if-aligned", ApiConsts.NAMESPC_DRBD_DISK_OPTIONS) == null)
                {
                    vlmDfnProps.setProp(
                        "discard-zeroes-if-aligned",
                        discardZerosIfAligned ? "yes" : "no",
                        ApiConsts.NAMESPC_DRBD_DISK_OPTIONS);
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "setting properties on volume",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError("Invalid hardcoded thin-volume related properties", exc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    public Flux<ApiCallRc> deployResources(ResponseContext context, Set<Resource> deployedResources)
    {
        return deployResources(context, deployedResources, true);
    }

    /**
     * Get currently online node ids.
     * @param rscDfn
     * @param accCtx
     * @return A Map with resource to nodeid mapping
     */
    public static Map<Resource, Integer> getOnlineNodeIds(ResourceDefinition rscDfn, AccessContext accCtx)
    {
        Map<Resource, Integer> onlineNodeIds = new HashMap<>();
        try
        {
            // deployedResources only contains the resources that were just created in the current API call, not the
            // already existing ones
            Iterator<Resource> rscIt = rscDfn.iterateResource(accCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();

                /*
                 * do NOT wait for resources that are
                 * * not online
                 * * diskless DRBD
                 * * not an active DRBD (i.e. nvme target, inactive, etc...)
                 */
                if (rsc.getNode().getPeer(accCtx).getConnectionStatus().equals(ConnectionStatus.ONLINE) &&
                    !rsc.getStateFlags().isSet(accCtx, Resource.Flags.DRBD_DISKLESS) &&
                    containsDrbdLayerData(rsc, accCtx))
                {
                    Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
                        rsc.getLayerData(accCtx),
                        DeviceLayerKind.DRBD
                    );
                    if (drbdRscDataSet.size() > 1)
                    {
                        throw new ImplementationError("Unexpected drbdRscDataSet size: " + drbdRscDataSet.size());
                    }
                    if (!drbdRscDataSet.isEmpty())
                    {
                        onlineNodeIds.put(
                            rsc,
                            ((DrbdRscData<Resource>) drbdRscDataSet.iterator().next()).getNodeId().value
                        );
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

        return onlineNodeIds;
    }

    public Publisher<ApiCallRc> waitResourcesReady(
        ResponseContext context,
        ResourceDefinition rscDfn,
        Set<Resource> deployedResources)
    {
        final ResourceName rscName = rscDfn.getName();
        Publisher<ApiCallRc> readyResponses;
        if (getVolumeDfnCountPrivileged(rscDfn) == 0)
        {
            // No DRBD resource is created when no volumes are present, so do not wait for it to be ready
            readyResponses = Mono.just(responseConverter.addContextAll(
                makeNoVolumesMessage(rscName), context, false));
        }
        else
        if (allDiskless(rscDfn))
        {
            readyResponses = Mono.just(makeAllDisklessMessage(rscName));
        }
        else
        {
            Map<Resource, Integer> onlineNodeIds = getOnlineNodeIds(rscDfn, apiCtx);

            List<Mono<ApiCallRc>> resourceReadyResponses = new ArrayList<>();
            if (rscDfn.getResourceCount() > 1)
            {
                // if we are the first resource, there is no other DRBD peer to connect.

                for (Resource rsc : deployedResources)
                {
                    if (AccessUtils.execPrivileged(
                        () -> DrbdLayerUtils.isAnyDrbdResourceExpected(apiCtx, rsc)
                    ))
                    {
                        NodeName nodeName = rsc.getNode().getName();
                        if (containsDrbdLayerData(rsc, peerAccCtx.get()))
                        {
                            Map<Resource, Integer> onlinePeerdNodeIds = new HashMap<>(onlineNodeIds);
                            onlinePeerdNodeIds.remove(rsc);

                            resourceReadyResponses.add(
                                eventWaiter
                                    .waitForStream(
                                        resourceStateEvent.get(),
                                        // TODO if anything is allowed above DRBD, this resource-name must be adjusted
                                        ObjectIdentifier.resource(nodeName, rscName)
                                    )
                                    .skipUntil(rscState -> rscState.isReady(onlinePeerdNodeIds.values()))
                                    .timeout(Duration.ofMillis(DFLT_RSC_READY_WAIT_TIME_IN_MS))
                                    .next()
                                    .thenReturn(makeResourceReadyMessage(context, nodeName, rscName))
                                    .onErrorResume(
                                        PeerNotConnectedException.class,
                                        ignored -> Mono.just(
                                            ApiCallRcImpl.singletonApiCallRc(
                                                ResponseUtils.makeNotConnectedWarning(nodeName)
                                            )
                                            )
                                        )
                                    .onErrorResume(TimeoutException.class, te -> makeRdyTimeoutApiRc(nodeName))
                            );
                        }
                    }
                }
            }

            readyResponses = Flux.merge(resourceReadyResponses);
        }
        return readyResponses;
    }

    /**
     * Deploy at least one resource of a resource definition to the satellites and wait for them to be ready.
     */
    public Flux<ApiCallRc> deployResources(
        ResponseContext context,
        Set<Resource> deployedResources,
        boolean waitForReady)
    {
        long rscDfnCount = deployedResources.stream()
            .map(Resource::getResourceDefinition)
            .map(ResourceDefinition::getName)
            .distinct()
            .count();
        if (rscDfnCount != 1)
        {
            throw new IllegalArgumentException("Resources belonging to precisely one resource definition expected");
        }

        ResourceDefinition rscDfn = deployedResources.iterator().next().getResourceDefinition();
        ResourceName rscName = rscDfn.getName();

        Set<NodeName> nodeNames = deployedResources.stream()
            .map(Resource::getNode)
            .map(Node::getName)
            .collect(Collectors.toSet());

        String nodeNamesStr = nodeNames.stream()
            .map(NodeName::getDisplayName)
            .map(displayName -> "''" + displayName + "''")
            .collect(Collectors.joining(", "));

        Publisher<ApiCallRc> readyResponses = waitForReady ?
            waitResourcesReady(context, rscDfn, deployedResources) : Flux.empty();

        Flux<ApiCallRc> nextSteps = setInitialized(deployedResources).concatWith(
            scheduleBackupService.fluxAllNewTasks(rscDfn, peerAccCtx.get())
        ).concatWith(ctrlRscDfnApiCallHandler.get().updateProps(rscDfn));

        return ctrlSatelliteUpdateCaller.updateSatellites(
            rscDfn,
            nextSteps
            // if failed, there is no need for the retry-task to wait for readyState
            // this is only true as long as there is no other flux concatenated after readyResponses
        )
            .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                updateResponses,
                rscName,
                nodeNames,
                "Created resource {1} on {0}",
                "Added peer(s) " + nodeNamesStr + " to resource {1} on {0}"
                )
            )
            .concatWith(readyResponses)
            .concatWith(nextSteps);
    }

    public Flux<ApiCallRc> setInitialized(Set<Resource> deployedResourcesRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Create resource",
                lockGuardFactory.buildDeferred(
                    LockType.WRITE,
                    LockObj.NODES_MAP,
                    LockObj.RSC_DFN_MAP,
                    LockObj.STOR_POOL_DFN_MAP
                ),
                () -> setInitializedInTransaction(deployedResourcesRef),
                MDC.getCopyOfContextMap()
            );
    }

    private Flux<ApiCallRc> setInitializedInTransaction(Set<Resource> deployedResourcesRef)
    {
        ResourceDefinition rscDfn = null;
        Flux<ApiCallRc> flux;
        try
        {
            AccessContext peerCtx = peerAccCtx.get();
            for (Resource rsc : deployedResourcesRef)
            {
                // rsc might have been deleted.
                // probably rsc-creation had a problem, resource got deleted again but the retryResource task still
                // tried to continue. if it somehow managed (race-condition?) we might end up here...
                // just ignore the resource and noop if needed.
                if (!rsc.isDeleted())
                {
                    if (rscDfn == null)
                    {
                        rscDfn = rsc.getResourceDefinition();
                    }
                    List<AbsRscLayerObject<Resource>> drbdRscList = LayerUtils
                        .getChildLayerDataByKind(rsc.getLayerData(peerCtx), DeviceLayerKind.DRBD);
                    for (AbsRscLayerObject<Resource> drbdRsc : drbdRscList)
                    {
                        ((DrbdRscData<Resource>) drbdRsc).getFlags().enableFlags(peerCtx, DrbdRscFlags.INITIALIZED);
                    }
                    allocationGranularityHelper.updateIfNeeded(rscDfn, false);
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "setting resource to initialized", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        if (rscDfn != null)
        {
            flux = ctrlSatelliteUpdateCaller.updateSatellites(
                rscDfn,
                null
            ).thenMany(Flux.empty());
            // user doesn't need info about setting an internal flag
        }
        else
        {
            flux = Flux.empty();
        }
        return flux;
    }

    private Mono<ApiCallRc> makeRdyTimeoutApiRc(NodeName nodeName)
    {
        final String msg = String.format(
            "Resource did not become ready on node '%s' within" +
            " reasonable time, check Satellite for errors.", nodeName);
        ApiCallRcImpl.ApiCallRcEntry apiEntry = ApiCallRcImpl.entryBuilder(
            ApiConsts.MASK_WARN | ApiConsts.MASK_RSC,
            msg
        )
            .putObjRef(ApiConsts.KEY_NODE, nodeName.displayValue)
            .build();
        return Mono.just(ApiCallRcImpl.singletonApiCallRc(apiEntry));
    }

    public ApiCallRc makeResourceDidNotAppearMessage(ResponseContext context)
    {
        return ApiCallRcImpl.singletonApiCallRc(responseConverter.addContext(ApiCallRcImpl.simpleEntry(
            ApiConsts.FAIL_UNKNOWN_ERROR,
            "Deployed resource did not appear"
        ), context, true));
    }

    public ApiCallRc makeEventStreamDisappearedUnexpectedlyMessage(ResponseContext context)
    {
        return ApiCallRcImpl.singletonApiCallRc(responseConverter.addContext(ApiCallRcImpl.simpleEntry(
            ApiConsts.FAIL_UNKNOWN_ERROR,
            "Resource disappeared while waiting for it to be ready"
        ), context, true));
    }

    Resource createResource(
        ResourceDefinition rscDfn,
        Node node,
        LayerPayload payload,
        long flags,
        List<DeviceLayerKind> layerStackRef
    )
    {
        if (!layerStackRef.isEmpty())
        {
            ensureLayerStackIsAllowed(layerStackRef);
        }

        Resource rsc;
        try
        {
            Resource.Flags[] initFlags = Resource.Flags.restoreFlags(flags);
            boolean isDiskless = false;
            for (Resource.Flags flag : initFlags)
            {
                if (flag == Resource.Flags.DISKLESS || flag == Resource.Flags.DRBD_DISKLESS)
                {
                    isDiskless = true;
                    break;
                }
            }
            if (!isDiskless)
            {
                // diskless resources do not need additional peer slots
                checkPeerSlotsForNewPeer(rscDfn);
            }

            rsc = resourceFactory.create(
                peerAccCtx.get(),
                rscDfn,
                node,
                payload,
                initFlags,
                layerStackRef
            );

            copyForceInitialSyncProp(rsc);

            List<DeviceLayerKind> unsupportedLayers = getUnsupportedLayers(peerAccCtx.get(), rsc);
            if (!unsupportedLayers.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_LAYER,
                        "Satellite '" + node.getName() + "' does not support the following layers: " + unsupportedLayers
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register the " + getRscDescriptionInline(node, rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.INFO_RSC_ALREADY_EXISTS,
                "A " + getRscDescriptionInline(node, rscDfn) + " already exists.",
                true
            ), dataAlreadyExistsExc);
        }
        return rsc;
    }

    private void copyForceInitialSyncProp(Resource rsc) throws AccessDeniedException, DatabaseException
    {
        ResourceDefinition rscDfn = rsc.getResourceDefinition();
        PriorityProps prioProps = new PriorityProps(
            rscDfn.getProps(peerAccCtx.get()),
            rscDfn.getResourceGroup().getProps(peerAccCtx.get()),
            ctrlPropsHelper.getStltPropsForView()
        );
        String forceSync = prioProps.getProp(ApiConsts.KEY_FORCE_INITIAL_SYNC, ApiConsts.NAMESPC_DRBD_OPTIONS);
        if (forceSync != null && !forceSync.isEmpty() && Boolean.parseBoolean(forceSync))
        {
            try
            {
                rscDfn.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA,
                        ApiConsts.VAL_TRUE,
                        ApiConsts.NAMESPC_DRBD_OPTIONS
                    );
            }
            catch (InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    Resource createResourceFromSnapshot(
        ResourceDefinition toRscDfn,
        Node toNode,
        Snapshot fromSnapshotRef,
        boolean fromBackup,
        Map<String, String> renameStorPoolMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        Resource rsc;
        try
        {
            checkPeerSlotsForNewPeer(toRscDfn);

            rsc = resourceFactory.create(
                peerAccCtx.get(),
                toRscDfn,
                toNode,
                fromSnapshotRef.getLayerData(peerAccCtx.get()),
                new Resource.Flags[0],
                fromBackup,
                renameStorPoolMap,
                apiCallRc
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register the " + getRscDescriptionInline(toNode, toRscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_RSC,
                    "A " + getRscDescriptionInline(toNode, toRscDfn) + " already exists.",
                    true
                ),
                dataAlreadyExistsExc
            );
        }
        return rsc;
    }

    static List<DeviceLayerKind> getUnsupportedLayers(AccessContext accCtx, Resource rsc) throws AccessDeniedException
    {
        List<DeviceLayerKind> usedDeviceLayerKinds = LayerUtils.getUsedDeviceLayerKinds(
            rsc.getLayerData(accCtx),
            accCtx
        );
        usedDeviceLayerKinds.removeAll(
            rsc.getNode()
                .getPeer(accCtx)
                .getExtToolsManager().getSupportedLayers()
        );

        return usedDeviceLayerKinds;
    }

    static void ensureLayerStackIsAllowed(List<DeviceLayerKind> layerStackRef)
    {
        if (!LayerUtils.isLayerKindStackAllowed(layerStackRef))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_LAYER_STACK,
                    "The layer stack " + layerStackRef + " is invalid"
                )
            );
        }
    }

    private VolumeDefinition loadVlmDfn(
        ResourceDefinition rscDfn,
        int vlmNr,
        boolean failIfNull
    )
    {
        return loadVlmDfn(rscDfn, LinstorParsingUtils.asVlmNr(vlmNr), failIfNull);
    }

    private VolumeDefinition loadVlmDfn(
        ResourceDefinition rscDfn,
        VolumeNumber vlmNr,
        boolean failIfNull
    )
    {
        VolumeDefinition vlmDfn;
        try
        {
            vlmDfn = rscDfn.getVolumeDfn(peerAccCtx.get(), vlmNr);

            if (failIfNull && vlmDfn == null)
            {
                String rscName = rscDfn.getName().displayValue;
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_VLM_DFN,
                        "Volume definition with number '" + vlmNr.value + "' on resource definition '" +
                            rscName + "' not found."
                    )
                    .setCause("The specified volume definition with number '" + vlmNr.value +
                        "' on resource definition '" + rscName + "' could not be found in the database")
                    .setCorrection("Create a volume definition with number '" + vlmNr.value +
                        "' on resource definition '" + rscName + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getVlmDfnDescriptionInline(rscDfn.getName().displayValue, vlmNr.value),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return vlmDfn;
    }

    private void checkPeerSlotsForNewPeer(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        int resourceCount = 0;
        Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (LayerUtils.hasLayer(rsc.getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD) &&
                !rsc.isDrbdDiskless(peerAccCtx.get()))
            {
                resourceCount++;
            }
        }

        rscIter = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIter.hasNext())
        {
            Resource otherRsc = rscIter.next();

            List<AbsRscLayerObject<Resource>> drbdRscDataList = LayerUtils.getChildLayerDataByKind(
                otherRsc.getLayerData(peerAccCtx.get()),
                DeviceLayerKind.DRBD
            );

            for (AbsRscLayerObject<Resource> rscLayerObj : drbdRscDataList)
            {
                if (((DrbdRscData<Resource>) rscLayerObj).getPeerSlots() < resourceCount)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                            "Resource on node " + otherRsc.getNode().getName().displayValue +
                            " has insufficient peer slots to add another peer"
                        )
                    );
                }
            }
        }
    }

    Iterator<VolumeDefinition> getVlmDfnIterator(ResourceDefinition rscDfn)
    {
        Iterator<VolumeDefinition> iterator;
        try
        {
            iterator = rscDfn.iterateVolumeDfn(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return iterator;
    }

    private int getVolumeDfnCountPrivileged(ResourceDefinition rscDfn)
    {
        int volumeDfnCount;
        try
        {
            volumeDfnCount = rscDfn.getVolumeDfnCount(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeDfnCount;
    }

    private boolean allDiskless(ResourceDefinition rscDfn)
    {
        boolean allDiskless = true;
        try
        {
            AccessContext accCtx = peerAccCtx.get();
            Iterator<Resource> rscIter = rscDfn.iterateResource(accCtx);
            while (rscIter.hasNext())
            {
                StateFlags<Flags> stateFlags = rscIter.next().getStateFlags();
                final boolean hasAnyDisklessFlagSet = stateFlags.isSomeSet(
                    accCtx,
                    Resource.Flags.DRBD_DISKLESS,
                    Resource.Flags.NVME_INITIATOR,
                    Resource.Flags.EBS_INITIATOR
                );
                if (!hasAnyDisklessFlagSet)
                {
                    allDiskless = false;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check diskless state of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return allDiskless;
    }

    private static boolean containsDrbdLayerData(Resource rsc, AccessContext accCtx)
    {
        boolean ret = false;
        try
        {
            List<AbsRscLayerObject<Resource>> drbdLayerDataSet = LayerUtils.getChildLayerDataByKind(
                rsc.getLayerData(accCtx),
                DeviceLayerKind.DRBD
            );
            for (AbsRscLayerObject<Resource> drbdData : drbdLayerDataSet)
            {
                if (!drbdData.hasAnyPreventExecutionIgnoreReason())
                {
                    ret = true;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "scan layer data for DRBD layer " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return ret;
    }

    private ApiCallRc makeResourceReadyMessage(
        ResponseContext context,
        NodeName nodeName,
        ResourceName rscName
    )
    {
        return ApiCallRcImpl.singletonApiCallRc(responseConverter.addContext(ApiCallRcImpl.simpleEntry(
            ApiConsts.CREATED,
            "Resource '" + rscName + "' on '" + nodeName + "' ready"
        ), context, true));
    }

    private ApiCallRcImpl makeNoVolumesMessage(ResourceName rscName)
    {
        return ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
            ApiConsts.WARN_NOT_FOUND,
            "No volumes have been defined for resource '" + rscName + "'"
        ));
    }

    private ApiCallRcImpl makeAllDisklessMessage(ResourceName rscName)
    {
        return ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
            ApiConsts.WARN_ALL_DISKLESS,
            "Resource '" + rscName + "' is unusable because it is diskless on all its nodes"
        ));
    }

    static List<DeviceLayerKind> getLayerStack(AccessContext accCtx, ResourceDefinition rscDfnRef)
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            layerStack = rscDfnRef.getLayerStack(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "accessing layerstack of " + getRscDfnDescriptionInline(rscDfnRef),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return layerStack;
    }

    static Set<List<DeviceLayerKind>> extractExistingLayerStacks(
        AccessContext accCtx,
        CtrlRscLayerDataFactory layerDataHelperRef,
        ResourceDefinition rscDfn
    )
    {
        Set<List<DeviceLayerKind>> ret;
        try
        {
            ret = rscDfn.streamResource(accCtx).map(
                layerDataHelperRef::getLayerStack
            ).collect(Collectors.toSet());

            /*
             * We might have a toggle-disk here were we just removed the layer-data.
             * Otherwise an empty layer list should not be possible anyways
             */
            ret.remove(Collections.emptyList());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "accessing resources of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return ret;
    }

    private boolean isNodeFlagSet(Node node, Node.Flags... flags)
    {
        boolean ret;
        try
        {
            ret = node.getFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "getting flag for node",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return ret;
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean ret;
        try
        {
            ret = rsc.getStateFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "getting flag for resource",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return ret;
    }

    private void disableFlags(Resource rsc, Resource.Flags... flags)
    {
        try
        {
            rsc.getStateFlags().disableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "disabling flags for resource",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private void disableFlags(Volume vlm, Volume.Flags... flags)
    {
        try
        {
            vlm.getFlags().disableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "disabling flags for volume",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private ApiCallRcImpl.ApiCallRcEntry makeFlaggedNvmeInitiatorWarning(StorPool storPool)
    {
        return makeFlaggedDiskless(storPool, "nvme initiator");
    }

    private ApiCallRcImpl.ApiCallRcEntry makeFlaggedDrbdDisklessWarning(StorPool storPool)
    {
        return makeFlaggedDiskless(storPool, "drbd diskless");
    }

    private ApiCallRcImpl.ApiCallRcEntry makeFlaggedDiskless(StorPool storPool, String type)
    {
        return ApiCallRcImpl
            .entryBuilder(
                MASK_WARN | MASK_STOR_POOL,
                "Resource will be automatically flagged as " + type
            )
            .setCause(
                String.format(
                    "Used storage pool '%s' is diskless, but resource was not flagged %s",
                    storPool.getName(),
                    type
                )
            )
            .build();
    }

}
