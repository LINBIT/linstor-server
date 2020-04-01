package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.StorPoolName;
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
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
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
    private final CtrlAutoStorPoolSelector autoStorPoolSelector;
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

    @Inject
    public CtrlRscAutoPlaceApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlAutoStorPoolSelector autoStorPoolSelectorRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodesMap nodesMapRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<CtrlRscAutoHelper> autoHelperProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        autoStorPoolSelector = autoStorPoolSelectorRef;
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

        return scopeRunner
            .fluxInTransactionalScope(
                "Auto-place resource",
                lockGuardFactory.buildDeferred(
                    LockType.WRITE,
                    LockObj.NODES_MAP, LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP
                ),
                () -> autoPlaceInTransaction(
                    rscNameStr,
                    selectFilter,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    Flux<ApiCallRc> autoPlaceInTransaction(
        String rscNameStr,
        AutoSelectFilterApi selectFilterRef,
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

        /*
         * If the resource is already deployed on X nodes, and the placement count now is Y:
         * case Y > X
         * only deploy (Y-X) additional resources, but on the previously selected storPoolName.
         * case Y == X
         * either NOP or additionally deploy disklessly on new nodes.
         * case Y < X
         * error.
         */
        List<Resource> alreadyPlaced = privilegedStreamResources(
            ctrlApiDataLoader.loadRscDfn(rscNameStr, true)
        )
            .filter(rsc ->
            {
                try
                {
                    return !rsc.getStateFlags().isSet(apiCtx, Resource.Flags.TIE_BREAKER);
                }
                catch (AccessDeniedException exc)
                {
                    throw new ImplementationError(exc);
                }
            })
            .collect(Collectors.toList());

        int additionalPlaceCount = Optional.ofNullable(
            mergedSelectFilter.getReplicaCount()
        ).orElse(0) - alreadyPlaced.size();

        if (additionalPlaceCount < 0)
        {
            throw new ApiRcException(makePlaceCountTooLowResponse(rscNameStr, alreadyPlaced));
        }

        List<String> storPoolNameList;
        if (
            alreadyPlaced.isEmpty() ||
                (mergedSelectFilter.getStorPoolNameList() != null &&
                !mergedSelectFilter.getStorPoolNameList().isEmpty())
        )
        {
            storPoolNameList = mergedSelectFilter.getStorPoolNameList();
        }
        else
        {
            storPoolNameList = Collections.singletonList(
                ctrlPropsHelper.getProps(alreadyPlaced.get(0)).map().get(InternalApiConsts.RSC_PROP_KEY_AUTO_SELECTED_STOR_POOL_NAME)
            );
        }

        errorReporter.logDebug(
            "Auto-placing '%s' on %d additional nodes" +
                (storPoolNameList == null ? "" : " using pool '" + storPoolNameList + "'"),
            rscNameStr,
            additionalPlaceCount
        );

        Flux<ApiCallRc> deploymentResponses;
        Flux<ApiCallRc> autoFlux;
        if (additionalPlaceCount == 0 && !mergedSelectFilter.getDisklessOnRemaining())
        {
            responseConverter.addWithDetail(responses, context,
                makeAlreadyDeployedResponse(
                    rscNameStr,
                    alreadyPlaced
                )
            );

            deploymentResponses = Flux.empty();
        }
        else
        {
            AutoStorPoolSelectorConfig autoStorPoolSelectorConfig = new AutoStorPoolSelectorConfig(
                additionalPlaceCount,
                mergedSelectFilter.getReplicasOnDifferentList(),
                mergedSelectFilter.getReplicasOnSameList(),
                mergedSelectFilter.getDoNotPlaceWithRscRegex(),
                Stream.concat(
                    mergedSelectFilter.getDoNotPlaceWithRscList().stream(),
                    // Do not attempt to re-use nodes that already have this resource
                    Stream.of(rscNameStr)
                ).collect(Collectors.toList()),
                storPoolNameList,
                mergedSelectFilter.getLayerStackList(),
                mergedSelectFilter.getProviderList()
            );

            final long rscSize = calculateResourceDefinitionSize(rscNameStr);

            Optional<Candidate> bestCandidate = findBestCandidate(
                autoStorPoolSelectorConfig,
                rscSize,
                // Try thick placement - do not override any free capacities
                Collections.emptyMap(),
                // Exclude thin storage pools
                false
            );

            if (bestCandidate.isPresent())
            {
                Candidate candidate = bestCandidate.get();

                Pair<List<Flux<ApiCallRc>>, Set<Resource>> deployedResources = createResources(
                    context,
                    responses,
                    rscNameStr,
                    mergedSelectFilter.getDisklessOnRemaining(),
                    candidate,
                    null,
                    mergedSelectFilter.getLayerStackList()
                );

                autoFlux = autoHelperProvider.get().manage(responses, context, rscNameStr).getFlux();

                ctrlTransactionHelper.commit();

                deploymentResponses = deployedResources.objB.isEmpty() ? Flux.empty()
                    : ctrlRscCrtApiHelper.deployResources(context, deployedResources.objB);
                deploymentResponses = Flux.merge(deployedResources.objA)
                    .concatWith(deploymentResponses)
                    .concatWith(autoFlux);
            }
            else
            {
                // Thick placement failed; ensure we haven't changed anything
                ctrlTransactionHelper.rollback();

                deploymentResponses = autoPlaceThin(
                    context,
                    rscNameStr,
                    storPoolNameList,
                    rscSize,
                    autoStorPoolSelectorConfig,
                    mergedSelectFilter.getDisklessOnRemaining(),
                    mergedSelectFilter.getLayerStackList()
                );
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

    private Flux<ApiCallRc> autoPlaceThin(
        ResponseContext context,
        String rscNameStr,
        List<String> storPoolNameList,
        long rscSize,
        AutoStorPoolSelectorConfig autoStorPoolSelectorConfig,
        Boolean disklessOnRemainingNodes,
        List<DeviceLayerKind> layerStackStrList
    )
    {
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
            .flatMapMany(freeCapacities -> scopeRunner
                .fluxInTransactionalScope(
                    "Auto-place resource including thin pools",
                    lockGuardFactory.buildDeferred(
                        LockType.WRITE,
                        LockObj.NODES_MAP, LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP),
                    () -> autoPlaceThinInTransaction(
                        context,
                        rscNameStr,
                        storPoolNameList,
                        rscSize,
                        autoStorPoolSelectorConfig,
                        disklessOnRemainingNodes,
                        freeCapacities,
                        layerStackStrList
                    )
                ));
    }

    private Flux<ApiCallRc> autoPlaceThinInTransaction(
        ResponseContext context,
        String rscNameStr,
        List<String> storPoolNameList,
        long rscSize,
        AutoStorPoolSelectorConfig autoStorPoolSelectorConfig,
        Boolean disklessOnRemainingNodes,
        Map<StorPool.Key, Long> thinFreeCapacities,
        List<DeviceLayerKind> layerStackList
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        Optional<Candidate> bestCandidate = findBestCandidate(
            autoStorPoolSelectorConfig,
            rscSize,
            thinFreeCapacities,
            true
        );

        Candidate candidate = bestCandidate
            .orElseThrow(() -> failNotEnoughCandidates(storPoolNameList, rscSize, autoStorPoolSelectorConfig));

        Pair<List<Flux<ApiCallRc>>, Set<Resource>> deployedResources = createResources(
            context,
            responses,
            rscNameStr,
            disklessOnRemainingNodes,
            candidate,
            thinFreeCapacities,
            layerStackList
        );

        Flux<ApiCallRc> autoFlux = autoHelperProvider.get().manage(responses, context, rscNameStr).getFlux();

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> deploymentResponses = deployedResources.objB.isEmpty() ?
            autoFlux :
            ctrlRscCrtApiHelper.deployResources(context, deployedResources.objB)
                .concatWith(autoFlux);

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(Flux.concat(deployedResources.objA))
            .concatWith(deploymentResponses);
    }

    private Optional<Candidate> findBestCandidate(
        AutoStorPoolSelectorConfig autoStorPoolSelectorConfig,
        long rscSize,
        Map<StorPool.Key, Long> freeCapacities,
        boolean includeThin
    )
    {
        Map<StorPoolName, List<Node>> availableStorPools = autoStorPoolSelector.listAvailableStorPools();

        Map<StorPoolName, List<Node>> usableStorPools = availableStorPools.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> filterUsableNodes(rscSize, freeCapacities, includeThin, entry.getKey(), entry.getValue())
            ));

        List<Candidate> candidates = autoStorPoolSelector.getCandidateList(
            usableStorPools,
            autoStorPoolSelectorConfig,
            FreeCapacityAutoPoolSelectorUtils.mostFreeCapacityNodeStrategy(freeCapacities)
        );

        return candidates.stream()
            .max(FreeCapacityAutoPoolSelectorUtils.mostFreeCapacityCandidateStrategy(peerAccCtx.get(), freeCapacities));
    }

    private List<Node> filterUsableNodes(
        long rscSize,
        Map<StorPool.Key, Long> freeCapacities,
        boolean includeThin,
        StorPoolName storPoolName,
        List<Node> nodes
    )
    {
        return nodes.stream()
            .filter(node -> FreeCapacityAutoPoolSelectorUtils.isStorPoolUsable(
                rscSize,
                freeCapacities,
                includeThin,
                storPoolName,
                node,
                apiCtx
            ).orElse(false))
            .collect(Collectors.toList());
    }

    private Pair<List<Flux<ApiCallRc>>, Set<Resource>> createResources(
        ResponseContext context,
        ApiCallRcImpl responses,
        String rscNameStr,
        Boolean disklessOnRemainingNodes,
        Candidate bestCandidate,
        Map<StorPool.Key, Long> thinFreeCapacities,
        List<DeviceLayerKind> layerStackList
    )
    {
        List<Flux<ApiCallRc>> autoFlux = new ArrayList<>();

        Map<String, String> rscPropsMap = new TreeMap<>();
        String selectedStorPoolName = bestCandidate.storPoolName.displayValue;
        rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, selectedStorPoolName);

        // FIXME: createResourceDb expects a list of String instead of a list of deviceLayerKinds...
        List<String> layerStackStrList = new ArrayList<>();
        for (DeviceLayerKind kind : layerStackList)
        {
            layerStackStrList.add(kind.name());
        }

        Set<Resource> deployedResources = new TreeSet<>();
        for (Node node : bestCandidate.nodes)
        {
            Pair<List<Flux<ApiCallRc>>, ApiCallRcWith<Resource>> createdRsc = ctrlRscCrtApiHelper.createResourceDb(
                node.getName().displayValue,
                rscNameStr,
                0L,
                rscPropsMap,
                Collections.emptyList(),
                null,
                thinFreeCapacities,
                layerStackStrList
            );
            Resource rsc = createdRsc.objB.extractApiCallRc(responses);
            autoFlux.addAll(createdRsc.objA);
            deployedResources.add(rsc);

            // bypass the whilteList
            ctrlPropsHelper.getProps(rsc).map().put(
                InternalApiConsts.RSC_PROP_KEY_AUTO_SELECTED_STOR_POOL_NAME,
                selectedStorPoolName
            );
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
                                layerStackStrList
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

        responseConverter.addWithOp(responses, context, ApiCallRcImpl
            .entryBuilder(
                ApiConsts.CREATED,
                "Resource '" + rscNameStr + "' successfully autoplaced on " +
                    bestCandidate.nodes.size() + " nodes"
            )
            .setDetails("Used storage pool: '" + bestCandidate.storPoolName.displayValue + "'\n" +
                "Used nodes: '" + bestCandidate.nodes.stream()
                .map(node -> node.getName().displayValue)
                .collect(Collectors.joining("', '")) + "'")
            .build()
        );

        return new Pair<>(autoFlux, deployedResources);
    }

    private long calculateResourceDefinitionSize(String rscNameStr)
    {
        long size = 0;
        try
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);
            Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(peerAccCtx.get());
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                size += vlmDfn.getVolumeSize(peerAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline(rscNameStr),
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
            ApiConsts.FAIL_INVLD_PLACE_COUNT,
            String.format(
                "The resource '%s' was already deployed on %d nodes: %s. " +
                    "The resource would have to be deleted from nodes to reach the placement count.",
                rscNameStr,
                alreadyPlaced.size(),
                alreadyPlaced.stream().map(rsc -> "'" + rsc.getNode().getName().displayValue + "'")
                    .collect(Collectors.joining(", "))
            )
        );
    }

    private ApiRcException failNotEnoughCandidates(
        List<String> storPoolNameList,
        final long rscSize,
        AutoStorPoolSelectorConfig config
    )
    {
        StringBuilder sb = new StringBuilder();
        sb.append("  Place Count: " + config.getPlaceCount() + "\n");
        if (!config.getReplicasOnDifferentList().isEmpty())
        {
            sb.append("  Replicas on different nodes: " + config.getReplicasOnDifferentList() + "\n");
        }
        if (!config.getReplicasOnSameList().isEmpty())
        {
            sb.append("  Replicas on same nodes: " + config.getReplicasOnSameList() + "\n");
        }
        if (config.getNotPlaceWithRscRegex() != null && !config.getNotPlaceWithRscRegex().isEmpty())
        {
            sb.append("  Don't place with resource (RegEx): " + config.getNotPlaceWithRscRegex() + "\n");
        }
        if (!config.getNotPlaceWithRscList().isEmpty())
        {
            sb.append("  Don't place with resource (List): " + config.getNotPlaceWithRscList() + "\n");
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
