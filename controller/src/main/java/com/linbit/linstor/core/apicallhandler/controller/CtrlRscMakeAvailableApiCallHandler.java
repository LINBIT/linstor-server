package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.ResourceWithPayloadPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceWithPayloadApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
        CtrlRscToggleDiskApiCallHandler toggleDiskHandlerRef
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
    }

    public Flux<ApiCallRc> makeResourceAvailable(
        String nodeNameRef,
        String rscNameRef,
        List<String> layerStackRef,
        boolean diskfulRef
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
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> makeRscAvailableInTransaction(
        String nodeNameRef,
        String rscNameRef,
        List<String> layerStackRef,
        boolean diskfulRef,
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

            if (isFlagSet(rsc, Resource.Flags.DELETE))
            {
                unsetFlag(rsc, Resource.Flags.DELETE);
                for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                {
                    unsetFlag(vlm, Volume.Flags.DELETE);
                }
            }

            if (isFlagSet(rsc, Resource.Flags.INACTIVE) && !isFlagSet(rsc, Resource.Flags.INACTIVE_PERMANENTLY))
            {
                Resource activeRsc = getActiveRsc(rscDfn);
                if (activeRsc == null) {
                    disableFlag(rsc, Resource.Flags.INACTIVE);
                    flux = stltUpdateCaller.updateSatellites(rsc, Flux.empty()).transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            updateResponses,
                            rsc.getResourceDefinition().getName(),
                            Collections.singleton(rsc.getNode().getName()),
                            "Resource activated on {0}",
                            "Resource activated on {0}"
                        )
                    );
                }
                else
                {
                    setFlag(activeRsc, Resource.Flags.INACTIVE);
                    flux = stltUpdateCaller.updateSatellites(activeRsc, Flux.empty()).transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            updateResponses,
                            rsc.getResourceDefinition().getName(),
                            Collections.singleton(rsc.getNode().getName()),
                            "Resource deactivated on {0}",
                            "Resource deactivated on {0}"
                        )
                    ).concatWith(postDeactivateOldRsc(rsc))
                        .onErrorResume(error -> abortDeactivateOldRsc(activeRsc, rsc));
                }
            }
            else
            {
                /*
                 * checking for DRBD_DISKLESS instead of DISKLESS to prevent NVMe and other cases.
                 * Toggle disk ONLY works with DRBD.
                 */
                if (isFlagSet(rsc, Resource.Flags.DRBD_DISKLESS) && diskfulRef)
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
                        false
                    );
                }
                else
                {
                    errorReporter.logTrace("Resource already in expected state. Nothing to do");
                    flux = Flux.just(
                        ApiCallRcImpl.singleApiCallRc(ApiConsts.MASK_SUCCESS, "Resource already deployed as requested")
                    );
                }
            }
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
                Resource activeRsc = getActiveRsc(rscDfn);
                setFlag(activeRsc, Resource.Flags.INACTIVE);
                flux = stltUpdateCaller.updateSatellites(activeRsc, Flux.empty()).transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        updateResponses,
                        rscDfn.getName(),
                        Collections.singleton(node.getName()),
                        "Resource deactivated on {0}",
                        "Resource deactivated on {0}"
                    )
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
                            () -> ctrlRscCrtApiCallHandler.createResource(Collections.singletonList(createRscPojo))
                        )
                    )
                ).onErrorResume(
                    error -> abortDeactivateOldRsc(activeRsc, rsc)
                        .concatWith(placeAnywhere(nodeNameRef, rscDfn, layerStack, diskfulRef))
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
                        () -> placeAnywhere(nodeNameRef, rscDfn, layerStack, diskfulRef)
                    )
                );
            }
            ctrlTransactionHelper.commit();
        }

        return flux;
    }

    private Flux<ApiCallRc> abortDeactivateOldRsc(Resource oldActiveRsc, Resource newActiveRsc)
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
            disableFlag(oldActiveRscRef, Resource.Flags.INACTIVE);

            ctrlTransactionHelper.commit();
            flux = stltUpdateCaller.updateSatellites(oldActiveRscRef, Flux.empty()).transform(
                updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    oldActiveRscRef.getResourceDefinition().getName(),
                    Collections.singleton(oldActiveRscRef.getNode().getName()),
                    "Resource reactivated on {0}",
                    "Resource reactivated on {0}"
                )
            );

        }
        return flux;
    }

    private Flux<ApiCallRc> postDeactivateOldRsc(Resource newActiveRsc)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Post deactivate rsc",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> postDeactivateOldRscInTransaction(newActiveRsc)
        );
    }

    private Flux<ApiCallRc> postDeactivateOldRscInTransaction(Resource newActiveRscRef)
    {
        disableFlag(newActiveRscRef, Resource.Flags.INACTIVE);
        ctrlTransactionHelper.commit();
        return stltUpdateCaller.updateSatellites(newActiveRscRef, Flux.empty()).transform(
            updateResponses -> CtrlResponseUtils.combineResponses(
                updateResponses,
                newActiveRscRef.getResourceDefinition().getName(),
                Collections.singleton(newActiveRscRef.getNode().getName()),
                "Resource activated on {0}",
                "Resource activated on {0}"
            )
        );
    }

    private Resource getActiveRsc(ResourceDefinition rscDfnRef)
    {
        try
        {
            Iterator<Resource> rscIt = rscDfnRef.iterateResource(peerCtxProvider.get());
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                if (!rsc.getStateFlags().isSet(peerCtxProvider.get(), Resource.Flags.INACTIVE))
                {
                    return rsc;
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "finding active resource " + CtrlRscDfnApiCallHandler.getRscDfnDescription(rscDfnRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return null;
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

    private void disableFlag(Resource rsc, Flags... flags)
    {
        try
        {
            rsc.getStateFlags().disableFlags(peerCtxProvider.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "disabling flags of " + CtrlRscApiCallHandler.getRscDescription(rsc),
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
        boolean diskfulRef
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
                diskfulRef
            )
        )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> placeAnywhereInTransaction(
        String nodeNameRef,
        ResourceDefinition rscDfn,
        List<DeviceLayerKind> layerStack,
        boolean diskfulRef
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
                 * creating the first
                 * diskful resource. However, this might still mean that other layers like NVMe are involved
                 */
                if (layerStack.contains(DeviceLayerKind.NVME) || layerStack.contains(DeviceLayerKind.OPENFLEX))
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
                    errorReporter.logTrace("Searching diskful storage pool for DRBD resource");
                    // default diskful DRBD setup with the given layers
                    autoSelect = createAutoSelectConfig(nodeNameRef, layerStack, null);
                    rscFlags = 0;
                    disklessForErrorMsg = false;
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

        Set<StorPool> storPoolSet = autoplacer.autoPlace(
            AutoSelectFilterPojo.merge(
                autoSelect,
                rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
            ),
            rscDfn,
            CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, peerCtxProvider.get())
        );

        StorPool sp = getStorPoolOrFail(storPoolSet, nodeNameRef, disklessForErrorMsg);
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
            null
        );
        ctrlTransactionHelper.commit();
        return ctrlRscCrtApiCallHandler.createResource(Collections.singletonList(createRscPojo));
    }

    private boolean hasDrbdDiskfulPeer(ResourceDefinition rscDfnRef)
    {
        return hasPeerWithoutFlag(rscDfnRef, Resource.Flags.DRBD_DISKLESS);
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

    private ResourceWithPayloadApi getSharedResourceCreationPojo(ResourceDefinition rscDfnRef, Node nodeRef)
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
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        );
                    }
                }
                if (allVolumesShared) {
                    Integer nodeId = null;
                    {
                        Set<AbsRscLayerObject<Resource>> drbdRscData = LayerRscUtils.getRscDataByProvider(
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
                            null
                        ),
                        LayerRscUtils.getLayerStack(rsc, peerCtxProvider.get()).stream()
                            .map(DeviceLayerKind::name).collect(Collectors.toList()),
                        nodeId
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
        Resource.Flags disklessFlag
    )
    {
        return new AutoSelectFilterPojo(
            0,
            1,
            Collections.singletonList(nodeName),
            null,
            null,
            null,
            null,
            null,
            null,
            layerStack,
            null,
            null,
            null,
            disklessFlag == null ? null : disklessFlag.name()
        );
    }

    private StorPool getStorPoolOrFail(Set<StorPool> storPoolSetRef, String nodeNameRef, boolean disklessRef)
    {
        if (storPoolSetRef == null)
        {
            throw failNoStorPoolFound(nodeNameRef, disklessRef);
        }
        if (storPoolSetRef.isEmpty())
        {
            throw failNoStorPoolFound(nodeNameRef, disklessRef);
        }
        if (storPoolSetRef.size() != 1)
        {
            throw new ImplementationError(
                "Only one storPool expected. got: " + storPoolSetRef.size() + ". " + storPoolSetRef
            );
        }
        return storPoolSetRef.iterator().next();
    }

    private ApiRcException failNoStorPoolFound(String nodeName, boolean diskless)
    {
        return new ApiRcException(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                "Autoplacer could not find " + (diskless ? "diskless" : "diskful") + " stor pool on node " + nodeName +
                    " matching resource-groups autoplace-settings"
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

    private List<DeviceLayerKind> getLayerStack(List<String> layerStackStr, ResourceDefinition rscDfnRef)
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

        if (layerStack == null || layerStack.isEmpty())
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
