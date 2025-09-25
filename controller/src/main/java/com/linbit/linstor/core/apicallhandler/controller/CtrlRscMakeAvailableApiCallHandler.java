package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
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
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

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
    private final Provider<AccessContext> peerCtxProvider;
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final CtrlApiDataLoader dataLoader;
    private final Autoplacer autoplacer;
    private final CtrlSatelliteUpdateCaller stltUpdateCaller;
    private final CtrlRscToggleDiskApiCallHandler toggleDiskHandler;
    private final SharedResourceManager sharedRscMgr;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;
    private final CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandler;
    private final RemoteMap remoteMap;

    @Inject
    public CtrlRscMakeAvailableApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        CtrlApiDataLoader dataLoaderRef,
        Autoplacer autoplacerRef,
        CtrlSatelliteUpdateCaller stltUpdateCallerRef,
        CtrlRscToggleDiskApiCallHandler toggleDiskHandlerRef,
        SharedResourceManager sharedRscMgrRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef,
        CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandlerRef,
        RemoteMap remoteMapRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        peerCtxProvider = peerCtxProviderRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        dataLoader = dataLoaderRef;
        autoplacer = autoplacerRef;
        stltUpdateCaller = stltUpdateCallerRef;
        toggleDiskHandler = toggleDiskHandlerRef;
        sharedRscMgr = sharedRscMgrRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
        ctrlRscActivateApiCallHandler = ctrlRscActivateApiCallHandlerRef;
        remoteMap = remoteMapRef;
    }

    public Flux<ApiCallRc> makeResourceAvailable(
        String nodeNameRef,
        String rscNameRef,
        List<String> layerStackRef,
        boolean diskfulRef,
        @Nullable List<Integer> drbdTcpPortsRef,
        boolean copyAllSnapsRef,
        @Nullable List<String> snapNamesToCopyRef
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
                    context
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
        @Nullable List<String> snapNamesToCopyRef,
        ResponseContext contextRef
    )
    {
        Flux<ApiCallRc> flux;

        ResourceDefinition rscDfn = dataLoader.loadRscDfn(rscNameRef, true);
        Resource rsc = dataLoader.loadRsc(nodeNameRef, rscNameRef, false);
        List<DeviceLayerKind> layerStack = getLayerStack(layerStackRef, rscDfn);

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
                if (activeRsc == null)
                {
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
                        CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, peerCtxProvider.get())
                    );
                    StorPool sp = getStorPoolOrFail(storPoolSet, nodeNameRef, false);

                    flux = toggleDiskHandler.resourceToggleDisk(
                        nodeNameRef,
                        rscNameRef,
                        sp.getName().displayValue,
                        null,
                        layerStackRef,
                        false,
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
            ResourceWithPayloadApi createRscPojo;
            // first, check if there is a shared storage pool already containing the shared resource on the given node

            Node node = dataLoader.loadNode(nodeNameRef, true);
            createRscPojo = getSharedResourceCreationPojo(rscDfn, node);
            if (createRscPojo != null)
            {
                errorReporter.logTrace("Trying to place new shared resource");

                // try to deactivate already active resource first
                Resource activeRsc = getActiveRsc(createRscPojo, node, rscDfn);
                flux = ctrlRscActivateApiCallHandler.deactivateRsc(
                    activeRsc.getNode().getName().displayValue,
                    activeRsc.getResourceDefinition().getName().displayValue
                ).concatWith(
                    freeCapacityFetcher.fetchThinFreeCapacities(Collections.singleton(node.getName())).flatMapMany(
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
                                snapNamesToCopyRef
                            )
                        )
                    )
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

        return flux;
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

    private @Nullable Resource getActiveRsc(Resource myRsc)
    {
        Resource activeRsc = null;
        ResourceDefinition rscDfn = myRsc.getResourceDefinition();
        try
        {
            TreeSet<Resource> sharedResources = sharedRscMgr.getSharedResources(myRsc);
            for (Resource rsc : sharedResources)
            {
                if (!myRsc.equals(rsc) && !rsc.getStateFlags().isSet(peerCtxProvider.get(), Resource.Flags.INACTIVE))
                {
                    activeRsc = rsc;
                    break;
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "finding active resource " + CtrlRscDfnApiCallHandler.getRscDfnDescription(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return activeRsc;
    }

    private @Nullable Resource getActiveRsc(ResourceWithPayloadApi rscToCreate, Node node, ResourceDefinition rscDfn)
    {
        Resource activeRsc = null;
        try
        {
            Set<SharedStorPoolName> sharedSpNames = getSharedSpNamesByRscCreateApi(rscToCreate, node, rscDfn);
            TreeSet<Resource> sharedResources = sharedRscMgr.getSharedResources(sharedSpNames, rscDfn);
            for (Resource rsc : sharedResources)
            {
                if (!rsc.getStateFlags().isSet(peerCtxProvider.get(), Resource.Flags.INACTIVE))
                {
                    activeRsc = rsc;
                    break;
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "finding active resource " + CtrlRscDfnApiCallHandler.getRscDfnDescription(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return activeRsc;
    }

    private HashSet<SharedStorPoolName> getSharedSpNamesByRscCreateApi(
        ResourceWithPayloadApi rscToCreateRef,
        Node node,
        ResourceDefinition rscDfn
    )
        throws AccessDeniedException
    {
        HashSet<SharedStorPoolName> ret = new HashSet<>();
        try
        {
            Set<String> storPoolNames = new HashSet<>();
            AccessContext peerCtx = peerCtxProvider.get();

            for (VolumeApi vlmApi : rscToCreateRef.getRscApi().getVlmList())
            {
                List<Map<String, String>> prioMaps = new ArrayList<>();
                prioMaps.add(vlmApi.getVlmProps());
                prioMaps.add(rscToCreateRef.getRscApi().getProps());
                prioMaps.add(
                    rscDfn.getVolumeDfn(
                        peerCtx,
                        LinstorParsingUtils.asVlmNr(vlmApi.getVlmNr())
                    )
                        .getProps(peerCtx).map()
                );
                prioMaps.add(rscDfn.getProps(peerCtx).map());
                prioMaps.add(rscDfn.getResourceGroup().getProps(peerCtx).map());
                prioMaps.add(node.getProps(peerCtx).map());

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
                StorPool sp = node.getStorPool(peerCtx, new StorPoolName(storPoolName));
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
            rsc.getStateFlags().enableFlags(peerCtxProvider.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "enabling flags of " + CtrlRscApiCallHandler.getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
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
        @Nullable List<String> snapNamesToCopyRef
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
        @Nullable List<String> snapNamesToCopyRef
    )
    {
        AutoSelectFilterPojo autoSelect = null;
        long rscFlags = 0;
        boolean disklessForErrorMsg = false;

        AccessContext peerAccCtx = peerCtxProvider.get();
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
                    Node node = dataLoader.loadNode(nodeNameRef, true);
                    boolean isEbsInitSupported;
                    boolean hasEbsTargetWithoutInit = false;
                    try
                    {
                        isEbsInitSupported = node.getPeer(peerAccCtx).getExtToolsManager()
                            .isProviderSupported(DeviceProviderKind.EBS_INIT);
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ApiAccessDeniedException(
                            exc,
                            "checking for EBS_INIT support",
                            ApiConsts.FAIL_ACC_DENIED_NODE
                        );
                    }
                    if (isEbsInitSupported)
                    {
                        String nodeName = node.getName().displayValue;
                        Iterator<StorPool> spIt;
                        try
                        {
                            spIt = node.iterateStorPools(peerAccCtx);
                            while (spIt.hasNext() && !hasEbsTargetWithoutInit)
                            {
                                StorPool sp = spIt.next();
                                if (sp.getDeviceProviderKind().equals(DeviceProviderKind.EBS_INIT))
                                {
                                    String az = RscStorageLayerHelper.getAvailabilityZone(peerAccCtx, remoteMap, sp);
                                    Resource targetEbsResource = RscStorageLayerHelper.findTargetEbsResource(
                                        peerAccCtx,
                                        remoteMap,
                                        rscDfn,
                                        az,
                                        nodeName
                                    );
                                    hasEbsTargetWithoutInit = targetEbsResource != null;
                                }
                            }
                        }
                        catch (AccessDeniedException exc1)
                        {
                            hasEbsTargetWithoutInit = false; // redundant, but for clarity
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
            CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, peerAccCtx)
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
                    CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, peerAccCtx)
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
            drbdTcpPortsRef
        );
        ctrlTransactionHelper.commit();
        return ctrlRscCrtApiCallHandler.createResource(
            Collections.singletonList(createRscPojo),
            Resource.DiskfulBy.MAKE_AVAILABLE,
            copyAllSnapsRef,
            snapNamesToCopyRef
        );
    }

    private boolean hasDrbdDiskfulPeer(ResourceDefinition rscDfnRef)
    {
        boolean ret;
        try
        {
            ret = !ResourceUtils.filterResourcesDrbdDiskfulActive(rscDfnRef, peerCtxProvider.get()).isEmpty();
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "checking active diskful DRBD peers for resource-definition: " + rscDfnRef.getName(),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
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

    private @Nullable ResourceWithPayloadApi getSharedResourceCreationPojo(ResourceDefinition rscDfnRef, Node nodeRef)
    {
        ResourceWithPayloadApi ret = null;
        try
        {
            // build Map<SharedStorPoolName, StorPool> of current node
            Map<SharedStorPoolName, StorPool> nodeStorPoolMap = new HashMap<>();
            {
                Iterator<StorPool> spIt = nodeRef.iterateStorPools(peerCtxProvider.get());
                while (spIt.hasNext())
                {
                    StorPool sp = spIt.next();
                    nodeStorPoolMap.put(sp.getSharedStorPoolName(), sp);
                }
            }

            Iterator<Resource> rscIt = rscDfnRef.iterateResource(peerCtxProvider.get());
            while (rscIt.hasNext() && ret == null)
            {
                Resource rsc = rscIt.next();
                boolean allVolumesShared = true;
                Iterator<Volume> vlmsIt = rsc.iterateVolumes();

                List<VolumeApi> vlmApiList = new ArrayList<>();

                while (vlmsIt.hasNext() && allVolumesShared)
                {
                    Volume vlm = vlmsIt.next();

                    Map<String, StorPool> storPoolMap = LayerVlmUtils.getStorPoolMap(vlm, peerCtxProvider.get());

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
                            rsc.getLayerData(peerCtxProvider.get()),
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
                            0,
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
                        LayerRscUtils.getLayerStack(rsc, peerCtxProvider.get()).stream()
                            .map(DeviceLayerKind::name).collect(Collectors.toList()),
                        nodeId,
                        null,
                        null
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "looking for shared resources of " + CtrlRscDfnApiCallHandler.getRscDfnDescription(rscDfnRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return ret;
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
        try
        {
            layerStack = LayerRscUtils.getLayerStack(rscRef, peerCtxProvider.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "getting layer stack of resource " + CtrlRscApiCallHandler.getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return layerStack;
    }

    private List<DeviceLayerKind> getLayerStack(@Nullable List<String> layerStackStr, ResourceDefinition rscDfnRef)
    {
        List<DeviceLayerKind> layerStack;
        if (layerStackStr == null || layerStackStr.isEmpty())
        {
            try
            {
                layerStack = rscDfnRef.getLayerStack(peerCtxProvider.get());
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiAccessDeniedException(
                    exc,
                    "accessing layer list of rscDfn " + rscDfnRef.getName(),
                    ApiConsts.FAIL_ACC_DENIED_RSC
                );
            }
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
        try
        {
            rscIt = rscDfnRef.iterateResource(peerCtxProvider.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "accessing resources of rscDfn " + rscDfnRef.getName(),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return rscIt;
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isSet;
        try
        {
            isSet = rsc.getStateFlags().isSet(peerCtxProvider.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "checking resource flags", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        return isSet;
    }

    private boolean isAnyFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isSet;
        try
        {
            isSet = rsc.getStateFlags().isSomeSet(peerCtxProvider.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "checking resource flags", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        return isSet;
    }

    private void unsetFlag(Resource rsc, Resource.Flags... flags)
    {
        try
        {
            rsc.getStateFlags().disableFlags(peerCtxProvider.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "disabling resource flags", ApiConsts.FAIL_ACC_DENIED_RSC);
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
            vlm.getFlags().disableFlags(peerCtxProvider.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "disabling volume flags", ApiConsts.FAIL_ACC_DENIED_VLM);
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
}
