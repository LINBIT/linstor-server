package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.AutoSelectorConfig;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
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
import java.util.stream.Stream;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscAutoPlaceApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final Autoplacer autoplacer;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodesMap nodesMap;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<CtrlRscAutoHelper> autoHelperProvider;
    private CtrlRscToggleDiskApiCallHandler toggleDiskHelper;

    @Inject
    public CtrlRscAutoPlaceApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        Autoplacer autoplacerRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodesMap nodesMapRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<CtrlRscAutoHelper> autoHelperProviderRef,
        CtrlRscToggleDiskApiCallHandler toggleDiskHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        autoplacer = autoplacerRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodesMap = nodesMapRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        autoHelperProvider = autoHelperProviderRef;
        toggleDiskHelper = toggleDiskHelperRef;
    }

    public Flux<ApiCallRc> autoPlace(
        String rscNameStr,
        AutoSelectFilterApi selectFilter
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            getObjectDescription(rscNameStr),
            getObjectDescriptionInline(rscNameStr),
            ApiConsts.MASK_RSC,
            objRefs
        );

        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet()).flatMapMany(
            // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
            // the freeCapacities parameter here
            ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                "Auto-place resource",
                lockGuardFactory.buildDeferred(
                    LockType.WRITE,
                    LockObj.NODES_MAP,
                    LockObj.RSC_DFN_MAP,
                    LockObj.STOR_POOL_DFN_MAP
                ),
                () -> autoPlaceInTransaction(
                    rscNameStr,
                    selectFilter,
                    context
                )
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    Flux<ApiCallRc> autoPlaceInTransaction(
        String rscNameStr,
        @Nullable AutoSelectFilterApi selectFilterRef,
        ResponseContext context
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);
        AutoSelectorConfig rscGrpSelectConfig = rscDfn.getResourceGroup().getAutoPlaceConfig();

        AutoSelectFilterPojo mergedSelectFilter = AutoSelectFilterPojo.merge(
            selectFilterRef,
            rscGrpSelectConfig.getApiData()
        );

        List<Resource> alreadyPlaced = privilegedStreamResources(
            ctrlApiDataLoader.loadRscDfn(rscNameStr, true)
        )
            .collect(Collectors.toList());

        alreadyPlaced = filterOnlyOneRscPerSharedSp(alreadyPlaced);

        List<Resource> alreadyPlacedDiskfulNotDeleting = new ArrayList<>();
        List<Resource> alreadyPlacedDisklessNotDeleting = new ArrayList<>();

        for (Resource rsc : alreadyPlaced)
        {
            // we do not care about deleting / evicted resources. just make sure to not count them
            if (
                !isSomeFlagSet(rsc, Resource.Flags.DELETE, Resource.Flags.EVICTED, Resource.Flags.EVACUATE) &&
                    !isNodeFlagSet(rsc, Node.Flags.EVACUATE)
            )
            {
                if (isFlagSet(rsc, Resource.Flags.DISKLESS))
                {
                    alreadyPlacedDisklessNotDeleting.add(rsc);
                }
                else
                {
                    alreadyPlacedDiskfulNotDeleting.add(rsc);
                }
            }
        }

        int additionalPlaceCount;
        if (
            mergedSelectFilter.getAdditionalReplicaCount() != null &&
            mergedSelectFilter.getAdditionalReplicaCount() > 0
        )
        {
            additionalPlaceCount = mergedSelectFilter.getAdditionalReplicaCount();
        }
        else
        {
            /*
             * If the resource is already deployed on X nodes, and the placement count now is Y:
             * case Y > X
             * only deploy (Y-X) additional resources, but on the previously selected storPoolName.
             * case Y == X
             * either NOP or additionally deploy disklessly on new nodes.
             * case Y < X
             * error.
             */
            if (mergedSelectFilter.getDisklessType() == null || mergedSelectFilter.getDisklessType().isEmpty())
            {
                additionalPlaceCount = Optional.ofNullable(
                    mergedSelectFilter.getReplicaCount()
                ).orElse(0) - (alreadyPlacedDiskfulNotDeleting.size());
            }
            else
            {
                additionalPlaceCount = Optional.ofNullable(
                    mergedSelectFilter.getReplicaCount()
                ).orElse(0) - alreadyPlacedDisklessNotDeleting.size();
            }
        }
        if (additionalPlaceCount < 0)
        {
            throw new ApiRcException(makePlaceCountTooLowResponse(rscNameStr, alreadyPlaced));
        }

        List<String> storPoolNameList = null;
        if (
            alreadyPlaced.isEmpty() ||
                (mergedSelectFilter.getStorPoolNameList() != null &&
                !mergedSelectFilter.getStorPoolNameList().isEmpty())
        )
        {
            storPoolNameList = mergedSelectFilter.getStorPoolNameList();
        }
        List<String> storPoolDisklessNameList = null;
        if (
            alreadyPlaced.isEmpty() ||
                (mergedSelectFilter.getStorPoolDisklessNameList() != null &&
                !mergedSelectFilter.getStorPoolDisklessNameList().isEmpty())
            )
        {
            storPoolDisklessNameList = mergedSelectFilter.getStorPoolDisklessNameList();
        }

        Flux<ApiCallRc> deploymentResponses;
        Flux<ApiCallRc> autoFlux;
        if (
            additionalPlaceCount == 0 &&
                (mergedSelectFilter.getDisklessOnRemaining() == null || !mergedSelectFilter.getDisklessOnRemaining())
        )
        {
            List<Resource> listForResponse;
            if (mergedSelectFilter.getDisklessType() == null || mergedSelectFilter.getDisklessType().isEmpty())
            {
                listForResponse = alreadyPlacedDiskfulNotDeleting;
            }
            else
            {
                listForResponse = alreadyPlacedDisklessNotDeleting;
            }
            responseConverter.addWithDetail(responses, context,
                makeAlreadyDeployedResponse(
                    rscNameStr,
                    listForResponse
                )
            );

            deploymentResponses = Flux.empty();
        }
        else
        {
            List<String> disklessNodeNames = alreadyPlacedDisklessNotDeleting.stream()
                .map(rsc -> rsc.getNode().getName().displayValue).collect(Collectors.toList());

            AutoSelectFilterPojo autoStorConfig = new AutoSelectFilterBuilder(mergedSelectFilter)
                .setAdditionalPlaceCount(additionalPlaceCount) // no forced place count, only additional place count
                .setStorPoolNameList(storPoolNameList)
                .setStorPoolDisklessNameList(storPoolDisklessNameList)
                .setDoNotPlaceWithRscList(Stream.concat(
                    mergedSelectFilter.getDoNotPlaceWithRscList().stream(),
                    // Do not attempt to re-use nodes that already have this resource
                    Stream.of(rscNameStr)
                ).collect(
                    Collectors.toList()
                ))
                .setSkipAlreadyPlacedOnNodeNamesCheck(disklessNodeNames)
                .build();

            final long rscSize = calculateResourceDefinitionSize(rscNameStr);

            Set<StorPool> candidate = findBestCandidate(
                autoStorConfig,
                rscDfn,
                rscSize
            );

            if (candidate != null)
            {
                Pair<List<Flux<ApiCallRc>>, Set<Resource>> deployedResources = createResources(
                    context,
                    responses,
                    rscNameStr,
                    mergedSelectFilter.getDisklessOnRemaining(),
                    candidate,
                    null,
                    mergedSelectFilter.getLayerStackList()
                );

                autoFlux = autoHelperProvider.get()
                    .manage(
                        new AutoHelperContext(responses, context, rscDfn)
                            .withSelectFilter(mergedSelectFilter)
                    )
                    .getFlux();

                ctrlTransactionHelper.commit();

                deploymentResponses = deployedResources.objB.isEmpty() ?
                    Flux.empty() :
                    ctrlRscCrtApiHelper.deployResources(context, deployedResources.objB);
                deploymentResponses = Flux.merge(deployedResources.objA)
                    .concatWith(deploymentResponses)
                    .concatWith(autoFlux);
            }
            else
            {
                throw failNotEnoughCandidates(storPoolNameList, rscSize, autoStorConfig);
            }
        }
        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(deploymentResponses)
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty())
            .onErrorResume(EventStreamTimeoutException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeResourceDidNotAppearMessage(context)))
            .onErrorResume(EventStreamClosedException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeEventStreamDisappearedUnexpectedlyMessage(context)));
    }

    private ArrayList<Resource> filterOnlyOneRscPerSharedSp(List<Resource> list)
    {
        Set<SharedStorPoolName> visitedSharedStorPoolNames = new HashSet<>();
        ArrayList<Resource> ret = new ArrayList<>();
        for (Resource rsc : list)
        {
            boolean sharedSpAlreadyCounted = false;
            Set<SharedStorPoolName> sharedSpNames = getSharedStorPoolNames(rsc);
            for (SharedStorPoolName sharedSpName : sharedSpNames)
            {
                if (visitedSharedStorPoolNames.contains(sharedSpName))
                {
                    sharedSpAlreadyCounted = true;
                    break;
                }
            }
            if (!sharedSpAlreadyCounted)
            {
                visitedSharedStorPoolNames.addAll(sharedSpNames);
                ret.add(rsc);
            }
        }

        return ret;
    }

    private Set<SharedStorPoolName> getSharedStorPoolNames(Resource rsc)
    {
        Set<SharedStorPoolName> sharedSpNames = new TreeSet<>();
        try
        {
            for (StorPool sp : LayerVlmUtils.getStorPools(rsc, apiCtx, false))
            {
                sharedSpNames.add(sp.getSharedStorPoolName());
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return sharedSpNames;
    }

    private Set<StorPool> findBestCandidate(
        AutoSelectFilterPojo autoStorConfigRef,
        @Nullable ResourceDefinition rscDfnRef,
        long rscSize
    )
    {
        return autoplacer.autoPlace(autoStorConfigRef, rscDfnRef, rscSize);
    }

    public Pair<List<Flux<ApiCallRc>>, Set<Resource>> createResources(
        ResponseContext context,
        ApiCallRcImpl responses,
        String rscNameStr,
        Boolean disklessOnRemainingNodes,
        Set<StorPool> selectedStorPoolSet,
        Map<StorPool.Key, Long> thinFreeCapacities,
        List<DeviceLayerKind> layerStackList
    )
    {
        List<Flux<ApiCallRc>> autoFlux = new ArrayList<>();

        Map<String, String> rscPropsMap = new TreeMap<>();

        // FIXME: createResourceDb expects a list of String instead of a list of deviceLayerKinds...
        List<String> layerStackStrList = new ArrayList<>();
        for (DeviceLayerKind kind : layerStackList)
        {
            layerStackStrList.add(kind.name());
        }

        Set<Resource> deployedResources = new TreeSet<>();
        for (StorPool storPool : selectedStorPoolSet)
        {
            String storPoolDisplayName = storPool.getName().displayValue;
            rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolDisplayName);

            Pair<List<Flux<ApiCallRc>>, ApiCallRcWith<Resource>> createdRsc = ctrlRscCrtApiHelper.createResourceDb(
                storPool.getNode().getName().displayValue,
                rscNameStr,
                0L,
                rscPropsMap,
                Collections.emptyList(),
                null,
                thinFreeCapacities,
                layerStackStrList,
                Resource.DiskfulBy.AUTO_PLACER
            );
            Resource rsc = createdRsc.objB.extractApiCallRc(responses);
            autoFlux.addAll(createdRsc.objA);
            deployedResources.add(rsc);
        }

        if (disklessOnRemainingNodes != null && disklessOnRemainingNodes)
        {
            // TODO: allow other diskless storage pools
            rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);

            // deploy resource disklessly on remaining nodes
            for (Node disklessNode : nodesMap.values())
            {
                try
                {
                    Resource deployedResource = disklessNode.getResource(
                        apiCtx,
                        LinstorParsingUtils.asRscName(rscNameStr)
                    );
                    if (
                        disklessNode.getNodeType(apiCtx) == Node.Type.SATELLITE && // only deploy on satellites
                            (deployedResource == null ||
                                deployedResource.getStateFlags().isSet(apiCtx, Resource.Flags.TIE_BREAKER))
                    )
                    {
                        Pair<List<Flux<ApiCallRc>>, ApiCallRcWith<Resource>> createdRsc = ctrlRscCrtApiHelper
                            .createResourceDb(
                                disklessNode.getName().displayValue,
                                rscNameStr,
                                Resource.Flags.DRBD_DISKLESS.flagValue | Resource.Flags.NVME_INITIATOR.flagValue,
                                rscPropsMap,
                                Collections.emptyList(),
                                null,
                                thinFreeCapacities,
                                layerStackStrList,
                                null
                        );
                        deployedResources.add(createdRsc.objB.extractApiCallRc(responses));
                        autoFlux.addAll(createdRsc.objA);
                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    throw new ApiAccessDeniedException(
                        accDeniedExc,
                        "access " + CtrlNodeApiCallHandler.getNodeDescriptionInline(disklessNode),
                        ApiConsts.FAIL_ACC_DENIED_NODE
                    );
                }
            }
        }

        int idx = 0;
        var objRefs = new HashMap<String, String>();
        for (var sp : selectedStorPoolSet)
        {
            objRefs.put("Node/" + idx, sp.getNode().getName().displayValue);
            objRefs.put(
                String.format("StoragePool/%d/0", idx),
                sp.getName().toString() + "," + sp.getDeviceProviderKind().toString());
            idx++;
        }

        ApiCallRc.RcEntry entry = ApiCallRcImpl
            .entryBuilder(
                ApiConsts.CREATED,
                "Resource '" + rscNameStr + "' successfully autoplaced on " +
                    selectedStorPoolSet.size() +
                    " nodes"
            )
            .setDetails(
                "Used nodes (storage pool name): '" +
                    selectedStorPoolSet.stream().map(
                            sp -> sp.getNode().getName().displayValue + " (" + sp.getName().displayValue + ")"
                        )
                        .collect(Collectors.joining("', '")) + "'")
            .putAllObjRefs(objRefs)
            .build();

        responseConverter.addWithOp(responses, context, entry);

        return new Pair<>(autoFlux, deployedResources);
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean flagSet = false;
        try
        {
            flagSet = rsc.getStateFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + CtrlRscApiCallHandler.getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return flagSet;
    }

    private boolean isSomeFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean flagSet = false;
        try
        {
            flagSet = rsc.getStateFlags().isSomeSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + CtrlRscApiCallHandler.getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return flagSet;
    }

    private boolean isNodeFlagSet(Resource rsc, Node.Flags... flags)
    {
        boolean flagSet = false;
        try
        {
            flagSet = rsc.getNode().getFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + CtrlRscApiCallHandler.getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return flagSet;
    }

    private long calculateResourceDefinitionSize(String rscNameStr)
    {
        return calculateResourceDefinitionSize(ctrlApiDataLoader.loadRscDfn(rscNameStr, true), peerAccCtx.get());
    }

    static long calculateResourceDefinitionSize(ResourceDefinition rscDfn, AccessContext accCtx)
    {
        long size = 0;
        try
        {
            Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(accCtx);
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                size += vlmDfn.getVolumeSize(accCtx);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline(rscDfn.getName().displayValue),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return size;

    }

    private Stream<Resource> privilegedStreamResources(ResourceDefinition rscDfn)
    {
        Stream<Resource> ret;
        try
        {
            ret = rscDfn.streamResource(apiCtx);
        }
        catch (AccessDeniedException accDenied)
        {
            throw new ImplementationError("ApiCtx has not enough privileges", accDenied);
        }
        return ret;
    }

    private ApiCallRcImpl.ApiCallRcEntry makeAlreadyDeployedResponse(
        String rscNameStr,
        List<Resource> alreadyPlaced
    )
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.WARN_RSC_ALREADY_DEPLOYED,
                "Resource '" + rscNameStr + "' was already deployed on " +
                    alreadyPlaced.size() + " nodes. Skipping."
            )
            .setDetails("Used nodes: '" +
                alreadyPlaced.stream().map(rsc -> rsc.getNode().getName().displayValue)
                    .collect(Collectors.joining("', '")) + "'")
            .build();
    }

    private ApiCallRcImpl.ApiCallRcEntry makePlaceCountTooLowResponse(
        String rscNameStr,
        List<Resource> alreadyPlaced
    )
    {
        return ApiCallRcImpl.simpleEntry(
            ApiConsts.WARN_RSC_ALREADY_DEPLOYED,
            String.format(
                "The resource '%s' was already deployed on %d nodes: %s. " +
                    "The resource would have to be deleted from nodes to reach the placement count.",
                rscNameStr,
                alreadyPlaced.size(),
                alreadyPlaced.stream().map(rsc -> "'" + rsc.getNode().getName().displayValue + "'")
                    .collect(Collectors.joining(", "))
            ),
            true
        );
    }

    private ApiRcException failNotEnoughCandidates(
        List<String> storPoolNameList,
        final long rscSize,
        AutoSelectFilterApi config
    )
    {
        StringBuilder sb = new StringBuilder();
        if (config.getReplicaCount() != null)
        {
            sb.append("  Place Count: " + config.getReplicaCount() + "\n");
        }
        else if (config.getAdditionalReplicaCount() != null)
        {
            sb.append("  Additional place count: " + config.getAdditionalReplicaCount() + "\n");
        }
        else
        {
            throw new ImplementationError("Placecount AND additional place count were empty!");
        }
        if (!config.getReplicasOnDifferentList().isEmpty())
        {
            sb.append("  Replicas on different nodes: " + config.getReplicasOnDifferentList() + "\n");
        }
        if (!config.getReplicasOnSameList().isEmpty())
        {
            sb.append("  Replicas on same nodes: " + config.getReplicasOnSameList() + "\n");
        }
        if (config.getDoNotPlaceWithRscRegex() != null && !config.getDoNotPlaceWithRscRegex().isEmpty())
        {
            sb.append("  Don't place with resource (RegEx): " + config.getDoNotPlaceWithRscRegex() + "\n");
        }
        if (!config.getDoNotPlaceWithRscList().isEmpty())
        {
            sb.append("  Don't place with resource (List): " + config.getDoNotPlaceWithRscList() + "\n");
        }
        if (config.getNodeNameList() != null && !config.getNodeNameList().isEmpty())
        {
            sb.append("  Node name: " + config.getNodeNameList() + "\n");
        }
        if (config.getStorPoolNameList() != null && !config.getStorPoolNameList().isEmpty())
        {
            sb.append("  Storage pool name: " + config.getStorPoolNameList() + "\n");
        }
        if (!config.getLayerStackList().isEmpty())
        {
            sb.append("  Layer stack: " + config.getLayerStackList() + "\n");
        }
        if (!config.getProviderList().isEmpty())
        {
            sb.append("  Provider: " + config.getProviderList() + "\n");
        }
        return new ApiRcException(ApiCallRcImpl
            .entryBuilder(
                ApiConsts.FAIL_NOT_ENOUGH_NODES,
                "Not enough available nodes"
            )
            .setDetails(
                "Not enough nodes fulfilling the following auto-place criteria:\n" +
                    (
                    storPoolNameList == null ||
                        storPoolNameList.isEmpty() ?
                            "" :
                            " * has a deployed storage pool named " + storPoolNameList + "\n" +
                                " * the storage pools have to have at least '" + rscSize +
                                "' free space\n"
                    ) +
                    " * the current access context has enough privileges to use the node and the storage pool\n" +
                        " * the node is online\n\n" +
                        "Auto-place configuration details:\n" + sb.toString()
            )
            .setSkipErrorReport(true)
            .build()
        );
    }

    private static String getObjectDescription(String rscNameStr)
    {
        return "Auto-placing resource: " + rscNameStr;
    }

    private static String getObjectDescriptionInline(String rscNameStr)
    {
        return "auto-placing resource: '" + rscNameStr + "'";
    }
}
