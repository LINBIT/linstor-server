package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.ResourceWithPayloadPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler.ToggleOp;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceWithPayloadApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.utils.ResourceUtils;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.layer.resource.RscStorageLayerHelper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscMakeAvailableApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ResponseConverter responseConverter;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final CtrlApiDataLoader dataLoader;
    private final Autoplacer autoplacer;
    private final CtrlSatelliteUpdateCaller stltUpdateCaller;
    private final CtrlRscToggleDiskApiCallHandler toggleDiskHandler;
    private final SharedResourceManager sharedRscMgr;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;
    private final CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandler;
    private final RemoteMap remoteMap;
    private final CtrlRscLiveMigrateHelper liveMigrateHelper;
    private final CtrlPropsHelper ctrlPropsHelper;

    @Inject
    public CtrlRscMakeAvailableApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        CtrlApiDataLoader dataLoaderRef,
        Autoplacer autoplacerRef,
        CtrlSatelliteUpdateCaller stltUpdateCallerRef,
        CtrlRscToggleDiskApiCallHandler toggleDiskHandlerRef,
        SharedResourceManager sharedRscMgrRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef,
        CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandlerRef,
        RemoteMap remoteMapRef,
        CtrlRscLiveMigrateHelper liveMigrateHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        dataLoader = dataLoaderRef;
        autoplacer = autoplacerRef;
        stltUpdateCaller = stltUpdateCallerRef;
        toggleDiskHandler = toggleDiskHandlerRef;
        sharedRscMgr = sharedRscMgrRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
        ctrlRscActivateApiCallHandler = ctrlRscActivateApiCallHandlerRef;
        remoteMap = remoteMapRef;
        liveMigrateHelper = liveMigrateHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
    }

    /**
     * Convenience overload of
     * {@link #makeResourceAvailable(String, String, List, boolean, List, boolean, List, boolean)} with
     * autoManageDualPrimaryRef set to false.
     */
    public Flux<ApiCallRc> makeResourceAvailable(
        String nodeNameRef,
        String rscNameRef,
        List<String> layerStackRef,
        boolean diskfulRef,
        @Nullable List<Integer> drbdTcpPortsRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef
    )
    {
        return makeResourceAvailable(
            nodeNameRef,
            rscNameRef,
            layerStackRef,
            diskfulRef,
            drbdTcpPortsRef,
            copyAllSnapsRef,
            snapNamesToCopyRef,
            false
        );
    }

    /**
     * Ensures that the given resource is usable on the given node, creating, activating or toggling it
     * as necessary.
     *
     * @param autoManageDualPrimaryRef if true, the resource is additionally prepared for a live migration
     *     from the node it is currently in use on (the migration source) to the given node: for DRBD
     *     resources allow-two-primaries (and protocol C if needed) is set between the two nodes, for
     *     resources in a shared storage pool the resource is activated on both nodes (for pools with
     *     external locking the satellites switch the LV locks to shared mode for that dual-active
     *     window). Reverted by unmake-available on the migration-source node. If the resource is not in
     *     use on any node there is no migration to prepare and the resource is simply made available, so
     *     clients that cannot distinguish a live-migration attach from a plain attach can always set the
     *     option.
     */
    public Flux<ApiCallRc> makeResourceAvailable(
        String nodeNameRef,
        String rscNameRef,
        List<String> layerStackRef,
        boolean diskfulRef,
        @Nullable List<Integer> drbdTcpPortsRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef,
        boolean autoManageDualPrimaryRef
    )
    {
        ResponseContext context = makeContext(nodeNameRef, rscNameRef);

        return scopeRunner.fluxInTransactionalScope(
                "Make resource available",
                lockGuardFactory.buildDeferred(
                    LockType.WRITE,
                    LockObj.NODES_MAP,
                    LockObj.RSC_DFN_MAP,
                    LockObj.STOR_POOL_DFN_MAP
                ),
                () -> makeRscAvailableInTransaction(
                    nodeNameRef,
                    rscNameRef,
                    layerStackRef,
                    diskfulRef,
                    drbdTcpPortsRef,
                    copyAllSnapsRef,
                    snapNamesToCopyRef,
                    autoManageDualPrimaryRef
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> makeRscAvailableInTransaction(
        String nodeNameRef,
        String rscNameRef,
        @Nullable List<String> layerStackRef,
        boolean diskfulRequestedRef,
        @Nullable List<Integer> drbdTcpPortsRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef,
        boolean autoManageDualPrimaryRef
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();

        ResourceDefinition rscDfn = dataLoader.loadRscDfn(rscNameRef);
        @Nullable Resource rsc = dataLoader.loadRscOrNull(nodeNameRef, rscNameRef);
        List<DeviceLayerKind> layerStack = getLayerStack(layerStackRef, rscDfn);
        Node node = dataLoader.loadNode(nodeNameRef);
        // if there is a shared storage pool already containing the shared resource on the given node,
        // the resource has to be created reusing the shared data instead of placing it anywhere
        @Nullable ResourceWithPayloadApi createRscPojo = rsc == null ?
            getSharedResourceCreationPojo(rscDfn, node) : null;

        @Nullable DualPrimaryPrep dualPrimaryPrep = null;
        if (autoManageDualPrimaryRef)
        {
            dualPrimaryPrep = prepareDualPrimary(rscDfn, rsc, node, createRscPojo, layerStack, nodeNameRef);
        }

        errorReporter.logTrace(
            "Making resource %s available on node %s. Already exists: %b",
            rscNameRef,
            nodeNameRef,
            rsc != null
        );
        if (rsc != null)
        {
            /*
             * For now, we can only perform some basic checks if the wanted resource looks like the existing one.
             * If not, response with an error RC.
             */
            if (layerStackRef != null && !layerStackRef.isEmpty() && !layerStack.equals(getDeployedLayerStack(rsc)))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_STACK,
                        "Layerstack of deployed resource does not match"
                    )
                );
            }

            boolean updateSatellite = false;
            if (isAnyFlagSet(rsc, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
            {
                unsetFlag(rsc, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE);
                for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                {
                    unsetFlag(vlm, Volume.Flags.DELETE, Volume.Flags.DRBD_DELETE);
                }
                updateSatellite = true;
            }

            if (isFlagSet(rsc, Resource.Flags.INACTIVE) && !isFlagSet(rsc, Resource.Flags.INACTIVE_PERMANENTLY))
            {
                Resource activeRsc = getActiveRsc(rsc);
                if (activeRsc == null || autoManageDualPrimaryRef)
                {
                    // with autoManageDualPrimary the currently active resource is the live-migration
                    // source and must stay active, resulting in both resources being active at once
                    flux = ctrlRscActivateApiCallHandler.activateRsc(
                        rsc.getNode().getName().displayValue,
                        rsc.getResourceDefinition().getName().displayValue
                    );
                }
                else
                {
                    flux = ctrlRscActivateApiCallHandler.deactivateRsc(
                        activeRsc.getNode().getName().displayValue,
                        activeRsc.getResourceDefinition().getName().displayValue
                    ).concatWith(
                        ctrlRscActivateApiCallHandler.activateRsc(
                            rsc.getNode().getName().displayValue,
                            rsc.getResourceDefinition().getName().displayValue
                        )
                    ).onErrorResume(error -> abortDeactivateOldRsc(activeRsc, rsc));
                }
                updateSatellite = false; // is done by active fluxes above
            }
            else
            {
                /*
                 * checking for DRBD_DISKLESS instead of DISKLESS to prevent NVMe and other cases.
                 * Toggle disk ONLY works with DRBD.
                 */
                if (isFlagSet(rsc, Resource.Flags.DRBD_DISKLESS) && diskfulRequestedRef)
                {
                    // toggle disk
                    AutoSelectFilterPojo autoSelect = createAutoSelectConfig(nodeNameRef, layerStack, null);
                    autoSelect.setSkipAlreadyPlacedOnNodeNamesCheck(Collections.singletonList(nodeNameRef));

                    Set<StorPool> storPoolSet = autoplacer.autoPlace(
                        AutoSelectFilterPojo.merge(
                            autoSelect,
                            rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                        ),
                        rscDfn,
                        CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn)
                    );
                    StorPool sp = getStorPoolOrFail(storPoolSet, nodeNameRef, false);

                    flux = toggleDiskHandler.resourceToggleDisk(
                        nodeNameRef,
                        rscNameRef,
                        sp.getName().displayValue,
                        null,
                        layerStackRef,
                        ToggleOp.INTO_DRBD_DISKFUL,
                        Resource.DiskfulBy.MAKE_AVAILABLE
                    );
                    updateSatellite = false;
                }
                else
                {
                    if (isFlagSet(rsc, Resource.Flags.TIE_BREAKER))
                    {
                        // unset TIE_BREAKER flag to mark resource as a wanted diskless
                        unsetFlag(rsc, Flags.TIE_BREAKER);
                        // TIE_BREAKER has DRBD_DISKLESS (which also has DISKLESS) flags + a new bit. we just removed
                        // all 3 bits, but we need to restore DISKLESS and DRBD_DISKLESS
                        setFlag(rsc, Flags.DRBD_DISKLESS);
                    }
                    // else we are either diskful, or diskless while diskfulRequested is false => noop

                    errorReporter.logTrace("Resource already in expected state. Nothing to do");
                    flux = Flux.just(
                        ApiCallRcImpl.singleApiCallRc(ApiConsts.MASK_SUCCESS, "Resource already deployed as requested")
                    );
                }

                if (updateSatellite)
                {
                    flux = flux.concatWith(
                        stltUpdateCaller.updateSatellites(rsc, Flux.empty())
                            .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2()));
                }
            }
            ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);

            ctrlTransactionHelper.commit();
        }
        else
        {
            if (createRscPojo != null)
            {
                errorReporter.logTrace("Trying to place new shared resource");

                @Nullable Resource activeRsc = getActiveRsc(createRscPojo, node, rscDfn);
                if (activeRsc != null && autoManageDualPrimaryRef)
                {
                    if (hasExternalLocking(activeRsc))
                    {
                        /*
                         * The active resource holds an exclusive lock on the shared volumes (e.g.
                         * lvmlockd). Its satellite only downgrades to shared locks once it sees the
                         * second leg, so create the new resource INACTIVE first (resource creation
                         * waits for all satellites of the rsc-dfn) and activate it afterwards, when
                         * the shared lock can be acquired.
                         */
                        flux = createSharedRsc(
                            node,
                            withAdditionalInitFlags(createRscPojo, Resource.Flags.INACTIVE),
                            copyAllSnapsRef,
                            snapNamesToCopyRef,
                            false
                        ).concatWith(
                            ctrlRscActivateApiCallHandler.activateRsc(nodeNameRef, rscNameRef)
                        );
                    }
                    else
                    {
                        // dual-active for a live migration: keep the source resource active and create
                        // the new resource active as well
                        flux = createSharedRsc(node, createRscPojo, copyAllSnapsRef, snapNamesToCopyRef, true);
                    }
                }
                else if (activeRsc != null)
                {
                    // try to deactivate already active resource first
                    flux = ctrlRscActivateApiCallHandler.deactivateRsc(
                        activeRsc.getNode().getName().displayValue,
                        activeRsc.getResourceDefinition().getName().displayValue
                    ).concatWith(
                        createSharedRsc(node, createRscPojo, copyAllSnapsRef, snapNamesToCopyRef, false)
                    ).onErrorResume(
                        error -> abortDeactivateOldRsc(activeRsc, null)
                            .concatWith(
                                placeAnywhere(
                                    nodeNameRef,
                                    rscDfn,
                                    layerStack,
                                    diskfulRequestedRef,
                                    drbdTcpPortsRef,
                                    copyAllSnapsRef,
                                    snapNamesToCopyRef
                                )
                            )
                    );
                }
                else
                {
                    // the shared resource is not active anywhere (e.g. its consumer was cleanly
                    // stopped): plain attach - create the resource reusing the shared data. Since no
                    // other copy is active the new resource stays active, ending with a usable device
                    flux = createSharedRsc(node, createRscPojo, copyAllSnapsRef, snapNamesToCopyRef, false);
                }
            }
            else
            {
                flux = freeCapacityFetcher.fetchThinFreeCapacities(Collections.singleton(node.getName())).flatMapMany(
                    // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
                    // the freeCapacities parameter here
                    ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                        "create resource",
                        lockGuardFactory.buildDeferred(
                            LockType.WRITE,
                            LockObj.NODES_MAP,
                            LockObj.RSC_DFN_MAP,
                            LockObj.STOR_POOL_DFN_MAP
                        ),
                        () -> placeAnywhere(
                            nodeNameRef,
                            rscDfn,
                            layerStack,
                            diskfulRequestedRef,
                            drbdTcpPortsRef,
                            copyAllSnapsRef,
                            snapNamesToCopyRef
                        )
                    )
                );
            }
            ctrlTransactionHelper.commit();
        }

        flux = appendDualPrimaryPropsFlux(flux, dualPrimaryPrep, rscNameRef, nodeNameRef);

        return flux;
    }

    /**
     * Appends the dual-primary property handling to the given flux: setting the DRBD net options for
     * the migration, or just an info that there is nothing to prepare. Noop for the shared storage pool
     * case (the dual-active handling is part of the regular create/activate paths) and if
     * autoManageDualPrimary was not requested at all (null prep).
     */
    private Flux<ApiCallRc> appendDualPrimaryPropsFlux(
        Flux<ApiCallRc> fluxRef,
        @Nullable DualPrimaryPrep dualPrimaryPrepRef,
        String rscNameRef,
        String tgtNodeNameRef
    )
    {
        Flux<ApiCallRc> flux = fluxRef;
        if (dualPrimaryPrepRef != null && dualPrimaryPrepRef.drbdMode)
        {
            if (dualPrimaryPrepRef.migrationSrcRsc != null)
            {
                flux = flux.concatWith(
                    applyDualPrimaryProps(
                        rscNameRef,
                        dualPrimaryPrepRef.migrationSrcRsc.getNode().getName().displayValue,
                        tgtNodeNameRef
                    )
                );
            }
            else
            {
                flux = flux.concatWith(
                    Flux.just(
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.MASK_INFO,
                            "Resource '" + rscNameRef + "' is not in use on another node, " +
                                "no dual-primary preparation needed"
                        )
                    )
                );
            }
        }
        return flux;
    }

    /**
     * Validations and migration-source lookup for autoManageDualPrimary.
     */
    private DualPrimaryPrep prepareDualPrimary(
        ResourceDefinition rscDfn,
        @Nullable Resource rsc,
        Node node,
        @Nullable ResourceWithPayloadApi createRscPojo,
        List<DeviceLayerKind> layerStack,
        String tgtNodeNameRef
    )
    {
        DualPrimaryPrep prep;
        boolean tgtShared = rsc != null ? liveMigrateHelper.hasSharedStorPool(rsc) : createRscPojo != null;
        if (tgtShared)
        {
            ensureSharedDualActiveSupported(rscDfn, rsc, node, createRscPojo);
            prep = new DualPrimaryPrep(false, null);
        }
        else
        {
            // migrationSrcRsc stays null if there is no migration to prepare: the resource is already
            // in use on the target node itself or not in use anywhere (plain attach)
            prep = new DualPrimaryPrep(
                true,
                findDualPrimaryMigrationSource(rscDfn, layerStack, tgtNodeNameRef)
            );
        }
        return prep;
    }

    /**
     * Creates the given resource (built by {@link #getSharedResourceCreationPojo}) on the given node,
     * reusing the data of its shared storage pool.
     *
     * @param allowDualActiveSharedRef see
     *     {@link CtrlRscCrtApiCallHandler#createResource(List, Resource.DiskfulBy, boolean, List, boolean, boolean)}
     */
    private Flux<ApiCallRc> createSharedRsc(
        Node node,
        ResourceWithPayloadApi createRscPojo,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef,
        boolean allowDualActiveSharedRef
    )
    {
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.singleton(node.getName()))
            .flatMapMany(
                // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
                // the freeCapacities parameter here
                ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                    "create resource",
                    lockGuardFactory.buildDeferred(
                        LockType.WRITE,
                        LockObj.NODES_MAP,
                        LockObj.RSC_DFN_MAP,
                        LockObj.STOR_POOL_DFN_MAP
                    ),
                    () -> ctrlRscCrtApiCallHandler.createResource(
                        Collections.singletonList(createRscPojo),
                        Resource.DiskfulBy.MAKE_AVAILABLE,
                        copyAllSnapsRef,
                        snapNamesToCopyRef,
                        false,
                        allowDualActiveSharedRef
                    )
                )
            );
    }

    /**
     * Validates that the shared resource of the given rsc-dfn may be activated on the migration source
     * and the target node at once. Only needed if the target resource has to be created or activated,
     * an already active target resource is handled by the regular noop path. If no shared resource is
     * active at all there is no migration source and therefore nothing to validate: the request is a
     * plain attach.
     */
    private void ensureSharedDualActiveSupported(
        ResourceDefinition rscDfn,
        @Nullable Resource rsc,
        Node node,
        @Nullable ResourceWithPayloadApi createRscPojo
    )
    {
        @Nullable Resource activeSharedRsc = null;
        if (rsc != null)
        {
            activeSharedRsc = getActiveRsc(rsc);
        }
        else if (createRscPojo != null)
        {
            activeSharedRsc = getActiveRsc(createRscPojo, node, rscDfn);
        }
        if (activeSharedRsc != null &&
            (rsc == null ||
                (isFlagSet(rsc, Resource.Flags.INACTIVE) &&
                    !isFlagSet(rsc, Resource.Flags.INACTIVE_PERMANENTLY))))
        {
            liveMigrateHelper.ensureSharedDualActiveSupported(activeSharedRsc);
        }
    }

    /**
     * Determines the live-migration source (the node the resource is currently in use on) for the
     * DRBD-based dual-primary preparation.
     *
     * @return the source resource, or null if there is no migration to prepare (the resource is in use
     *     on the target node itself or not in use anywhere)
     */
    private @Nullable Resource findDualPrimaryMigrationSource(
        ResourceDefinition rscDfn,
        List<DeviceLayerKind> layerStack,
        String tgtNodeNameRef
    )
    {
        if (!layerStack.contains(DeviceLayerKind.DRBD))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_LAYER_STACK,
                    "auto_manage_dual_primary requires DRBD (or a shared storage pool)",
                    true
                )
            );
        }
        return liveMigrateHelper.findMigrationSource(rscDfn, tgtNodeNameRef);
    }

    private Flux<ApiCallRc> applyDualPrimaryProps(
        String rscNameRef,
        String srcNodeNameRef,
        String tgtNodeNameRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Set dual-primary properties",
            lockGuardFactory.buildDeferred(
                LockType.WRITE,
                LockObj.NODES_MAP,
                LockObj.RSC_DFN_MAP
            ),
            () -> applyDualPrimaryPropsInTransaction(rscNameRef, srcNodeNameRef, tgtNodeNameRef)
        );
    }

    private Flux<ApiCallRc> applyDualPrimaryPropsInTransaction(
        String rscNameRef,
        String srcNodeNameRef,
        String tgtNodeNameRef
    )
    {
        Resource srcRsc = dataLoader.loadRsc(srcNodeNameRef, rscNameRef);
        Resource tgtRsc = dataLoader.loadRsc(tgtNodeNameRef, rscNameRef);

        ApiCallRcImpl responses = liveMigrateHelper.setDualPrimaryProps(srcRsc, tgtRsc);

        ctrlTransactionHelper.commit();

        return Flux.<ApiCallRc>just(responses)
            .concatWith(
                stltUpdateCaller.updateSatellites(srcRsc.getResourceDefinition(), Flux.empty())
                    .transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            updateResponses,
                            srcRsc.getResourceDefinition().getName(),
                            "Updated DRBD net options for the live migration on {0}"
                        )
                    )
            );
    }

    private Flux<ApiCallRc> abortDeactivateOldRsc(Resource oldActiveRsc, @Nullable Resource newActiveRsc)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Abort deactivate old rsc",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> abortDeactivateOldRscInTransaction(oldActiveRsc, newActiveRsc)
        );
    }

    private Flux<ApiCallRc> abortDeactivateOldRscInTransaction(
        Resource oldActiveRscRef,
        @Nullable Resource newActiveRscRef
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        if (newActiveRscRef == null || isFlagSet(newActiveRscRef, Resource.Flags.INACTIVE))
        {
            flux = ctrlRscActivateApiCallHandler.activateRsc(
                oldActiveRscRef.getNode().getName().displayValue,
                oldActiveRscRef.getResourceDefinition().getName().displayValue
            );
        }
        return flux;
    }

    private boolean hasExternalLocking(Resource rsc)
    {
        boolean extLocking = false;
        for (StorPool sp : LayerVlmUtils.getStorPools(rsc))
        {
            if (sp.isExternalLocking())
            {
                extLocking = true;
                break;
            }
        }
        return extLocking;
    }

    private @Nullable Resource getActiveRsc(Resource myRsc)
    {
        Resource activeRsc = null;
        TreeSet<Resource> sharedResources = sharedRscMgr.getSharedResources(myRsc);
        for (Resource rsc : sharedResources)
        {
            if (!myRsc.equals(rsc) && !rsc.getStateFlags().isSet(Resource.Flags.INACTIVE))
            {
                activeRsc = rsc;
                break;
            }
        }
        return activeRsc;
    }

    private @Nullable Resource getActiveRsc(ResourceWithPayloadApi rscToCreate, Node node, ResourceDefinition rscDfn)
    {
        Resource activeRsc = null;
        Set<SharedStorPoolName> sharedSpNames = getSharedSpNamesByRscCreateApi(rscToCreate, node, rscDfn);
        TreeSet<Resource> sharedResources = sharedRscMgr.getSharedResources(sharedSpNames, rscDfn);
        for (Resource rsc : sharedResources)
        {
            if (!rsc.getStateFlags().isSet(Resource.Flags.INACTIVE))
            {
                activeRsc = rsc;
                break;
            }
        }
        return activeRsc;
    }

    private HashSet<SharedStorPoolName> getSharedSpNamesByRscCreateApi(
        ResourceWithPayloadApi rscToCreateRef,
        Node node,
        ResourceDefinition rscDfn
    )
    {
        HashSet<SharedStorPoolName> ret = new HashSet<>();
        try
        {
            Set<String> storPoolNames = new HashSet<>();

            for (VolumeApi vlmApi : rscToCreateRef.getRscApi().getVlmList())
            {
                List<Map<String, String>> prioMaps = new ArrayList<>();
                prioMaps.add(vlmApi.getVlmProps());
                prioMaps.add(rscToCreateRef.getRscApi().getProps());
                prioMaps.add(
                    rscDfn.getVolumeDfn(
                        LinstorParsingUtils.asVlmNr(vlmApi.getVlmNr())
                    )
                        .getProps().map()
                );
                prioMaps.add(rscDfn.getProps().map());
                prioMaps.add(rscDfn.getResourceGroup().getProps().map());
                prioMaps.add(node.getProps().map());

                String poolName = get(prioMaps, ApiConsts.KEY_STOR_POOL_NAME);

                if (poolName != null)
                {
                    storPoolNames.add(poolName);
                }

                String drbdMetaStorPoolName = get(prioMaps, ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
                if (drbdMetaStorPoolName != null)
                {
                    storPoolNames.add(drbdMetaStorPoolName);
                }
            }

            for (String storPoolName : storPoolNames)
            {
                StorPool sp = node.getStorPool(new StorPoolName(storPoolName));
                ret.add(sp.getSharedStorPoolName());
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    private @Nullable String get(List<Map<String, String>> prioMapsRef, String key)
    {
        String value = null;
        for (Map<String, String> map : prioMapsRef)
        {
            value = map.get(key);
            if (value != null)
            {
                break;
            }
        }
        return value;
    }

    private void setFlag(Resource rsc, Flags... flags)
    {
        try
        {
            rsc.getStateFlags().enableFlags(flags);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private Flux<ApiCallRc> placeAnywhere(
        String nodeNameRef,
        ResourceDefinition rscDfnRef,
        List<DeviceLayerKind> layerStackRef,
        boolean diskfulRef,
        @Nullable List<Integer> drbdTcpPortsRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef
    )
    {
        ResponseContext context = makeContext(nodeNameRef, rscDfnRef.getName().displayValue);

        return scopeRunner.fluxInTransactionalScope(
            "Place anywhere on node",
            lockGuardFactory.buildDeferred(
                LockType.WRITE,
                LockObj.NODES_MAP,
                LockObj.RSC_DFN_MAP,
                LockObj.STOR_POOL_DFN_MAP
            ),
            () -> placeAnywhereInTransaction(
                nodeNameRef,
                rscDfnRef,
                layerStackRef,
                diskfulRef,
                drbdTcpPortsRef,
                copyAllSnapsRef,
                snapNamesToCopyRef
            )
        )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> placeAnywhereInTransaction(
        String nodeNameRef,
        ResourceDefinition rscDfn,
        List<DeviceLayerKind> layerStack,
        boolean diskfulRef,
        @Nullable List<Integer> drbdTcpPortsRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef
    )
    {
        AutoSelectFilterPojo autoSelect = null;
        long rscFlags = 0;
        boolean disklessForErrorMsg = false;

        if (layerStack.contains(DeviceLayerKind.DRBD))
        {
            if (!diskfulRef && hasDrbdDiskfulPeer(rscDfn))
            {
                errorReporter.logTrace("Searching diskless storage pool for DRBD resource");
                // we can create a DRBD diskless resource
                autoSelect = createAutoSelectConfig(
                    nodeNameRef,
                    layerStack,
                    Resource.Flags.DRBD_DISKLESS
                );

                rscFlags = Resource.Flags.DRBD_DISKLESS.flagValue;
                disklessForErrorMsg = true;
            }
            else
            {
                /*
                 * No diskful peer (or forced diskful). "make resource available" is interpreted in this case as
                 * creating the first diskful resource. However, this might still mean that other layers like NVMe are
                 * involved
                 */
                if (layerStack.contains(DeviceLayerKind.NVME))
                {
                    if (hasNvmeTarget(rscDfn))
                    {
                        errorReporter.logTrace(
                            "Searching diskless storage pool for DRBD over NVME (initiator) resource"
                        );
                        // we want to connect as initiator
                        autoSelect = createAutoSelectConfig(
                            nodeNameRef,
                            layerStack,
                            Resource.Flags.NVME_INITIATOR
                        );
                        rscFlags = Resource.Flags.NVME_INITIATOR.flagValue;
                        disklessForErrorMsg = true;
                    }
                }
                else
                {
                    Node node = dataLoader.loadNode(nodeNameRef);
                    boolean isEbsInitSupported;
                    boolean hasEbsTargetWithoutInit = false;
                    isEbsInitSupported = node.getPeer().getExtToolsManager()
                        .isProviderSupported(DeviceProviderKind.EBS_INIT);
                    if (isEbsInitSupported)
                    {
                        String nodeName = node.getName().displayValue;
                        Iterator<StorPool> spIt = node.iterateStorPools();
                        while (spIt.hasNext() && !hasEbsTargetWithoutInit)
                        {
                            StorPool sp = spIt.next();
                            if (sp.getDeviceProviderKind().equals(DeviceProviderKind.EBS_INIT))
                            {
                                String az = RscStorageLayerHelper.getAvailabilityZone(
                                    remoteMap,
                                    sp,
                                    ctrlPropsHelper.getCtrlPropsForView()
                                );
                                Resource targetEbsResource = RscStorageLayerHelper.findTargetEbsResource(
                                    remoteMap,
                                    rscDfn,
                                    az,
                                    nodeName,
                                    ctrlPropsHelper.getCtrlPropsForView()
                                );
                                hasEbsTargetWithoutInit = targetEbsResource != null;
                            }
                        }
                    }
                    if (hasEbsTargetWithoutInit)
                    {
                        errorReporter.logTrace(
                            "Searching diskless storage pool for DRBD over EBS (initiator) resource"
                        );
                        // we want to connect as initiator
                        autoSelect = createAutoSelectConfig(
                            nodeNameRef,
                            layerStack,
                            Resource.Flags.EBS_INITIATOR
                        );
                        rscFlags = Resource.Flags.EBS_INITIATOR.flagValue;
                        disklessForErrorMsg = true;
                    }
                    else
                    {
                        errorReporter.logTrace("Searching diskful storage pool for DRBD resource");
                        // default diskful DRBD setup with the given layers
                        autoSelect = createAutoSelectConfig(nodeNameRef, layerStack, null);
                        rscFlags = 0;
                        disklessForErrorMsg = false;
                    }
                }
            }
        }

        if (autoSelect == null)
        {
            // TODO: this will change once shared SP is merged into master

            // default diskful setup with the given layers
            autoSelect = createAutoSelectConfig(nodeNameRef, layerStack, null);
            rscFlags = 0;
            disklessForErrorMsg = false;
        }

        @Nullable Set<StorPool> storPoolSet = autoplacer.autoPlace(
            AutoSelectFilterPojo.merge(
                autoSelect,
                rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
            ),
            rscDfn,
            CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn)
        );

        @Nullable StorPool sp = getStorPoolOrNull(storPoolSet);
        if (sp == null)
        {
            // if diskless assignment, run autoplacer again without resource group restrictions
            if (disklessForErrorMsg)
            {
                storPoolSet = autoplacer.autoPlace(
                    autoSelect,
                    rscDfn,
                    CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn)
                );
                sp = getStorPoolOrNull(storPoolSet);
            }

            if (sp == null)
            {
                throw failNoStorPoolFound(nodeNameRef, disklessForErrorMsg);
            }
        }
        ResourceWithPayloadPojo createRscPojo = new ResourceWithPayloadPojo(
            new RscPojo(
                rscDfn.getName().displayValue,
                nodeNameRef,
                rscFlags,
                Collections.singletonMap(
                    ApiConsts.KEY_STOR_POOL_NAME,
                    sp.getName().displayValue
                )
            ),
            layerStack.stream().map(DeviceLayerKind::name).collect(Collectors.toList()),
            null,
            autoSelect.getDrbdPortCount(),
            drbdTcpPortsRef,
            // rscs via make-available are always treated as drbd-clients (unless the given SP is diskful, but that is
            // handled in the createResource ACH)
            true
        );
        ctrlTransactionHelper.commit();
        return ctrlRscCrtApiCallHandler.createResource(
            Collections.singletonList(createRscPojo),
            Resource.DiskfulBy.MAKE_AVAILABLE,
            copyAllSnapsRef,
            snapNamesToCopyRef,
            false
        );
    }

    private boolean hasDrbdDiskfulPeer(ResourceDefinition rscDfnRef)
    {
        boolean ret;
        ret = !ResourceUtils.filterResourcesDrbdDiskfulActive(rscDfnRef).isEmpty();
        return ret;
    }

    private boolean hasNvmeTarget(ResourceDefinition rscDfnRef)
    {
        return hasPeerWithoutFlag(rscDfnRef, Resource.Flags.NVME_INITIATOR);
    }

    private boolean hasPeerWithoutFlag(ResourceDefinition rscDfn, Resource.Flags flag)
    {
        Iterator<Resource> rscIt = getRscIter(rscDfn);
        boolean foundPeer = false;
        while (rscIt.hasNext())
        {
            Resource peerRsc = rscIt.next();
            if (!isFlagSet(peerRsc, flag))
            {
                foundPeer = true;
                break;
            }
        }
        return foundPeer;
    }

    private @Nullable ResourceWithPayloadApi getSharedResourceCreationPojo(
        ResourceDefinition rscDfnRef,
        Node nodeRef
    )
    {
        @Nullable ResourceWithPayloadApi ret = null;
        // build Map<SharedStorPoolName, StorPool> of current node
        Map<SharedStorPoolName, StorPool> nodeStorPoolMap = new HashMap<>();
        {
            Iterator<StorPool> spIt = nodeRef.iterateStorPools();
            while (spIt.hasNext())
            {
                StorPool sp = spIt.next();
                nodeStorPoolMap.put(sp.getSharedStorPoolName(), sp);
            }
        }

        Iterator<Resource> rscIt = rscDfnRef.iterateResource();
        while (rscIt.hasNext() && ret == null)
        {
            Resource rsc = rscIt.next();
            boolean allVolumesShared = true;
            Iterator<Volume> vlmsIt = rsc.iterateVolumes();

            List<VolumeApi> vlmApiList = new ArrayList<>();

            while (vlmsIt.hasNext() && allVolumesShared)
            {
                Volume vlm = vlmsIt.next();

                Map<String, StorPool> storPoolMap = LayerVlmUtils.getStorPoolMap(vlm);

                // we need to ensure DATA and (if exists) DRBD_META paths are shared. the other storage pools can be
                // recreated

                StorPool dataSp = storPoolMap.get(RscLayerSuffixes.SUFFIX_DATA);
                StorPool drbdMetaSp = storPoolMap.get(RscLayerSuffixes.SUFFIX_DRBD_META);

                StorPool sharedDataSpFromNode = nodeStorPoolMap.get(dataSp.getSharedStorPoolName());
                StorPool sharedDrbdMetaSpFromNode = null;
                if (drbdMetaSp != null)
                {
                    sharedDrbdMetaSpFromNode = nodeStorPoolMap.get(drbdMetaSp.getSharedStorPoolName());
                }

                Map<String, String> vlmsProps = new HashMap<>();
                if (sharedDataSpFromNode != null)
                {
                    vlmsProps.put(
                        ApiConsts.KEY_STOR_POOL_NAME,
                        sharedDataSpFromNode.getName().displayValue
                    );
                    if (drbdMetaSp != null)
                    {
                        if (sharedDrbdMetaSpFromNode != null)
                        {
                            vlmsProps.put(
                                ApiConsts.KEY_STOR_POOL_DRBD_META_NAME,
                                sharedDrbdMetaSpFromNode.getName().displayValue
                            );
                        }
                        else
                        {
                            allVolumesShared = false;
                        }
                    }
                }
                else
                {
                    allVolumesShared = false;
                }
                if (allVolumesShared)
                {
                    vlmApiList.add(
                        new VlmPojo(
                            null,
                            null,
                            null,
                            vlm.getVolumeNumber().value,
                            0,
                            vlmsProps,
                            Optional.empty(),
                            Optional.empty(),
                            null,
                            null,
                            null,
                            null
                        )
                    );
                }
            }
            if (allVolumesShared)
            {
                Integer nodeId = null;
                {
                    Set<AbsRscLayerObject<Resource>> drbdRscData = LayerRscUtils.getRscDataByLayer(
                        rsc.getLayerData(),
                        DeviceLayerKind.DRBD
                    );
                    if (drbdRscData.size() == 1)
                    {
                        nodeId = ((DrbdRscData<Resource>) drbdRscData.iterator().next()).getNodeId().value;
                    }
                    else if (drbdRscData.size() > 1)
                    {
                        throw new ImplementationError("Unexpected drbdRscData count: " + drbdRscData.size());
                    }
                }
                ret = new ResourceWithPayloadPojo(
                    new RscPojo(
                        rscDfnRef.getName().displayValue,
                        nodeRef.getName().displayValue,
                        null,
                        null,
                        null,
                        0L,
                        Collections.emptyMap(),
                        vlmApiList,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                    ),
                    LayerRscUtils.getLayerStack(rsc).stream()
                        .map(DeviceLayerKind::name).collect(Collectors.toList()),
                    nodeId,
                    null,
                    null,
                    // rscs via make-available are always treated as drbd-clients, unless they are shared-resources
                    // in that case they should not be diskless at all, but shared diskful (regardless if DRBD is
                    // used or not)
                    null
                );
            }
        }
        return ret;
    }

    /**
     * Copies the given creation pojo (as built by {@link #getSharedResourceCreationPojo}) with the
     * given resource init flag additionally set, since the flags of a {@link RscPojo} are immutable.
     */
    private ResourceWithPayloadApi withAdditionalInitFlags(
        ResourceWithPayloadApi createRscPojoRef,
        Resource.Flags flagRef
    )
    {
        ResourceApi rscApi = createRscPojoRef.getRscApi();
        return new ResourceWithPayloadPojo(
            new RscPojo(
                rscApi.getName(),
                rscApi.getNodeName(),
                rscApi.getNodeUuid(),
                null,
                rscApi.getUuid(),
                rscApi.getFlags() | flagRef.flagValue,
                rscApi.getProps(),
                new ArrayList<>(rscApi.getVlmList()),
                null,
                null,
                null,
                null,
                rscApi.getLayerData(),
                rscApi.getCreateTimestamp().orElse(null),
                rscApi.getEffectivePropsPojo()
            ),
            createRscPojoRef.getLayerStack(),
            createRscPojoRef.getDrbdNodeId(),
            createRscPojoRef.getPortCount(),
            createRscPojoRef.getPorts(),
            createRscPojoRef.isDrbdClient()
        );
    }

    private AutoSelectFilterPojo createAutoSelectConfig(
        String nodeName,
        List<DeviceLayerKind> layerStack,
        @Nullable Resource.Flags disklessFlag
    )
    {
        return new AutoSelectFilterBuilder()
            .setPlaceCount(0)
            .setAdditionalPlaceCount(1)
            .setNodeNameList(Collections.singletonList(nodeName))
            .setLayerStackList(layerStack)
            .setDisklessType(disklessFlag == null ? null : disklessFlag.name())
            .build();
    }

    static @Nullable StorPool getStorPoolOrNull(@Nullable Set<StorPool> storPoolSetRef)
    {
        if (storPoolSetRef == null)
        {
            return null;
        }
        if (storPoolSetRef.isEmpty())
        {
            return null;
        }
        if (storPoolSetRef.size() != 1)
        {
            throw new ImplementationError(
                "Only one storPool expected. got: " + storPoolSetRef.size() + ". " + storPoolSetRef
            );
        }
        return storPoolSetRef.iterator().next();
    }

    static StorPool getStorPoolOrFail(@Nullable Set<StorPool> storPoolSetRef, String nodeName, boolean diskless)
    {
        @Nullable StorPool sp = getStorPoolOrNull(storPoolSetRef);
        if (sp == null)
        {
            throw failNoStorPoolFound(nodeName, diskless);
        }
        return sp;
    }

    static ApiRcException failNoStorPoolFound(String nodeName, boolean diskless)
    {
        return new ApiRcException(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                "Autoplacer could not find " + (diskless ? "diskless" : "diskful") + " stor pool " +
                    (nodeName == null ? "" : "on node " + nodeName) +
                    " matching resource-groups autoplace-settings",
                    true
            )
        );
    }

    private List<DeviceLayerKind> getDeployedLayerStack(Resource rscRef)
    {
        List<DeviceLayerKind> layerStack;
        layerStack = LayerRscUtils.getLayerStack(rscRef);
        return layerStack;
    }

    private List<DeviceLayerKind> getLayerStack(@Nullable List<String> layerStackStr, ResourceDefinition rscDfnRef)
    {
        List<DeviceLayerKind> layerStack;
        if (layerStackStr == null || layerStackStr.isEmpty())
        {
            layerStack = rscDfnRef.getLayerStack();
        }
        else
        {
            layerStack = LinstorParsingUtils.asDeviceLayerKind(layerStackStr);
        }

        if (layerStack.isEmpty())
        {
            layerStack = Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);
        }
        return layerStack;
    }

    private Iterator<Resource> getRscIter(ResourceDefinition rscDfnRef)
    {
        Iterator<Resource> rscIt;
        rscIt = rscDfnRef.iterateResource();
        return rscIt;
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isSet;
        isSet = rsc.getStateFlags().isSet(flags);
        return isSet;
    }

    private boolean isAnyFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isSet;
        isSet = rsc.getStateFlags().isSomeSet(flags);
        return isSet;
    }

    private void unsetFlag(Resource rsc, Resource.Flags... flags)
    {
        try
        {
            rsc.getStateFlags().disableFlags(flags);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

    }

    private void unsetFlag(Volume vlm, Volume.Flags... flags)
    {
        try
        {
            vlm.getFlags().disableFlags(flags);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private ResponseContext makeContext(String nodeName, String rscName)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeName);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscName);

        return new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            "Node: " + nodeName + ", Resource: '" + rscName + "'",
            "resource '" + rscName + "' on node " + nodeName + "",
            ApiConsts.MASK_RSC,
            objRefs
        );
    }

    /**
     * Result of the autoManageDualPrimary validations: whether the dual-primary window is managed via
     * DRBD net options (in contrast to a dual-active shared resource) and if so, the migration-source
     * resource (null if there is no migration to prepare).
     */
    private static class DualPrimaryPrep
    {
        private final boolean drbdMode;
        private final @Nullable Resource migrationSrcRsc;

        DualPrimaryPrep(boolean drbdModeRef, @Nullable Resource migrationSrcRscRef)
        {
            drbdMode = drbdModeRef;
            migrationSrcRsc = migrationSrcRscRef;
        }
    }
}
