package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

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
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
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
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    public Flux<ApiCallRc> autoPlace(
        String rscNameStr,
        AutoSelectFilterApi selectFilter,
        boolean disklessOnRemainingNodes
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
                LockGuard.createDeferred(
                    nodesMapLock.writeLock(),
                    rscDfnMapLock.writeLock(),
                    storPoolDfnMapLock.writeLock()
                ),
                () -> autoPlaceInTransaction(rscNameStr, selectFilter, disklessOnRemainingNodes, context)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> autoPlaceInTransaction(
        String rscNameStr,
        AutoSelectFilterApi selectFilter,
        boolean disklessOnRemainingNodes,
        ResponseContext context
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        /*
         * If the resource is already deployed on X nodes, and the placement count now is Y:
         * case Y > X
         * only deploy (Y-X) additional resources, but on the previously selected storPoolName.
         * case Y == X
         * either NOP or additionally deploy disklessly on new nodes.
         * case Y < X
         * error.
         */
        List<Resource> alreadyPlaced =
            privilegedStreamResources(ctrlApiDataLoader.loadRscDfn(rscNameStr, true)).collect(Collectors.toList());

        int additionalPlaceCount = selectFilter.getPlaceCount() - alreadyPlaced.size();

        if (additionalPlaceCount < 0)
        {
            throw new ApiRcException(makePlaceCountTooLowResponse(rscNameStr, alreadyPlaced));
        }

        String storPoolName;
        if (alreadyPlaced.isEmpty() || selectFilter.getStorPoolNameStr() != null)
        {
            storPoolName = selectFilter.getStorPoolNameStr();
        }
        else
        {
            storPoolName = ctrlPropsHelper.getProps(alreadyPlaced.get(0)).map()
                .get(InternalApiConsts.RSC_PROP_KEY_AUTO_SELECTED_STOR_POOL_NAME);
        }

        errorReporter.logDebug(
            "Auto-placing '%s' on %d additional nodes" +
                (storPoolName == null ? "" : " using pool '" + storPoolName + "'"),
            rscNameStr,
            additionalPlaceCount
        );

        Flux<ApiCallRc> deploymentResponses;
        if (additionalPlaceCount == 0 && !disklessOnRemainingNodes)
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
                selectFilter.getReplicasOnDifferentList(),
                selectFilter.getReplicasOnSameList(),
                selectFilter.getNotPlaceWithRscRegex(),
                Stream.concat(
                    selectFilter.getNotPlaceWithRscList().stream(),
                    // Do not attempt to re-use nodes that already have this resource
                    Stream.of(rscNameStr)
                ).collect(Collectors.toList()),
                storPoolName
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

                List<Resource> deployedResources = createResources(
                    context,
                    responses,
                    rscNameStr,
                    disklessOnRemainingNodes,
                    candidate,
                    null
                );

                ctrlTransactionHelper.commit();

                deploymentResponses = deployedResources.isEmpty() ?
                    Flux.empty() :
                    ctrlRscCrtApiHelper.deployResources(context, deployedResources);
            }
            else
            {
                // Thick placement failed; ensure we haven't changed anything
                ctrlTransactionHelper.rollback();

                deploymentResponses = autoPlaceThin(
                    context,
                    rscNameStr,
                    storPoolName,
                    rscSize,
                    autoStorPoolSelectorConfig,
                    disklessOnRemainingNodes
                );
            }
        }

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(deploymentResponses)
            .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty())
            .onErrorResume(EventStreamTimeoutException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeResourceDidNotAppearMessage(context)))
            .onErrorResume(EventStreamClosedException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeEventStreamDisappearedUnexpectedlyMessage(context)));
    }

    private Flux<ApiCallRc> autoPlaceThin(
        ResponseContext context,
        String rscNameStr,
        String storPoolName,
        long rscSize,
        AutoStorPoolSelectorConfig autoStorPoolSelectorConfig,
        boolean disklessOnRemainingNodes
    )
    {
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
            .flatMapMany(freeCapacities -> scopeRunner
                .fluxInTransactionalScope(
                    "Auto-place resource including thin pools",
                    LockGuard.createDeferred(
                        nodesMapLock.writeLock(),
                        rscDfnMapLock.writeLock(),
                        storPoolDfnMapLock.writeLock()
                    ),
                    () -> autoPlaceThinInTransaction(
                        context,
                        rscNameStr,
                        storPoolName,
                        rscSize,
                        autoStorPoolSelectorConfig,
                        disklessOnRemainingNodes,
                        freeCapacities
                    )
                ));
    }

    private Flux<ApiCallRc> autoPlaceThinInTransaction(
        ResponseContext context,
        String rscNameStr,
        String storPoolName,
        long rscSize,
        AutoStorPoolSelectorConfig autoStorPoolSelectorConfig,
        boolean disklessOnRemainingNodes,
        Map<StorPool.Key, Long> thinFreeCapacities
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
            .orElseThrow(() -> failNotEnoughCandidates(storPoolName, rscSize));

        List<Resource> deployedResources = createResources(
            context,
            responses,
            rscNameStr,
            disklessOnRemainingNodes,
            candidate,
            thinFreeCapacities
        );

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> deploymentResponses = deployedResources.isEmpty() ?
            Flux.empty() :
            ctrlRscCrtApiHelper.deployResources(context, deployedResources);

        return Flux
            .<ApiCallRc>just(responses)
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

    private List<Resource> createResources(
        ResponseContext context,
        ApiCallRcImpl responses,
        String rscNameStr,
        boolean disklessOnRemainingNodes,
        Candidate bestCandidate,
        Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        Map<String, String> rscPropsMap = new TreeMap<>();
        String selectedStorPoolName = bestCandidate.storPoolName.displayValue;
        rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, selectedStorPoolName);

        List<Resource> deployedResources = new ArrayList<>();
        for (Node node : bestCandidate.nodes)
        {
            ResourceData rsc = ctrlRscCrtApiHelper.createResourceDb(
                node.getName().displayValue,
                rscNameStr,
                0L,
                rscPropsMap,
                Collections.emptyList(),
                null,
                thinFreeCapacities
            ).extractApiCallRc(responses);
            deployedResources.add(rsc);

            // bypass the whilteList
            ctrlPropsHelper.getProps(rsc).map().put(
                InternalApiConsts.RSC_PROP_KEY_AUTO_SELECTED_STOR_POOL_NAME,
                selectedStorPoolName
            );
        }

        if (disklessOnRemainingNodes)
        {
            // TODO: allow other diskless storage pools
            rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);

            // deploy resource disklessly on remaining nodes
            for (Node disklessNode : nodesMap.values())
            {
                try
                {
                    if (disklessNode.getNodeType(apiCtx) == Node.NodeType.SATELLITE && // only deploy on satellites
                        disklessNode.getResource(apiCtx, LinstorParsingUtils.asRscName(rscNameStr)) == null)
                    {
                        deployedResources.add(
                            ctrlRscCrtApiHelper.createResourceDb(
                                disklessNode.getName().displayValue,
                                rscNameStr,
                                RscFlags.DISKLESS.flagValue,
                                rscPropsMap,
                                Collections.emptyList(),
                                null,
                                thinFreeCapacities
                            ).extractApiCallRc(responses)
                        );
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

        return deployedResources;
    }

    private long calculateResourceDefinitionSize(String rscNameStr)
    {
        long size = 0;
        try
        {
            ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);
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
                alreadyPlaced.stream().map(rsc -> rsc.getAssignedNode().getName().displayValue)
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
                alreadyPlaced.stream().map(rsc -> "'" + rsc.getAssignedNode().getName().displayValue + "'")
                    .collect(Collectors.joining(", "))
            )
        );
    }

    private ApiRcException failNotEnoughCandidates(String storPoolName, final long rscSize)
    {
        return new ApiRcException(ApiCallRcImpl
            .entryBuilder(
                ApiConsts.FAIL_NOT_ENOUGH_NODES,
                "Not enough available nodes"
            )
            .setDetails(
                "Not enough nodes fulfilling the following auto-place criteria:\n" +
                    (
                        storPoolName == null ?
                            "" :
                            " * has a deployed storage pool named '" + storPoolName + "'\n" +
                                " * the storage pool '" + storPoolName + "' has to have at least '" +
                                rscSize + "' free space\n"
                    ) +
                    " * the current access context has enough privileges to use the node and the storage pool\n" +
                    " * the node is online"
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
