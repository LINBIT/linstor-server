package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionData;
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
         * if the user already called an autoPlace with X replica,
         * and now calls another autoPlace for the same resource with Y replica:
         * case Y == X again
         * either NOP or additionally deploy disklessly on new nodes
         * case Y > X
         * only deploy (Y-X) additionally resources, but on the previously selected
         * storPoolName.
         * case Y < X
         * Error.
         *
         * Y is our selectFilter.getPlaceCount(). We have to determine X now
         */
        String upperRscName = rscNameStr.toUpperCase();
        List<Resource> alreadyPlaced =
            nodesMap.values().stream()
                .flatMap(node -> privilegedStreamResources(node))
                .filter(rsc -> rsc.getDefinition().getName().value.equals(upperRscName))
                .collect(Collectors.toList());

        AutoStorPoolSelectorConfig selectorCfg = new AutoStorPoolSelectorConfig(selectFilter);
        boolean nop = false;

        if (!alreadyPlaced.isEmpty())
        {
            String selectedStorPoolName =
                ctrlPropsHelper.getProps(alreadyPlaced.get(0)).map()
                    .get(InternalApiConsts.RSC_PROP_KEY_AUTO_SELECTED_STOR_POOL_NAME);
            if (alreadyPlaced.size() == selectFilter.getPlaceCount())
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_RSC_ALREADY_DEPLOYED,
                        "Resource '" + rscNameStr + "' was already deployed on " +
                            selectFilter.getPlaceCount() + " nodes. Skipping."
                    )
                    .setDetails("Used storage pool: '" + selectedStorPoolName + "'\n" +
                        "Used nodes: '" +
                        alreadyPlaced.stream().map(rsc -> rsc.getAssignedNode().getName().displayValue)
                            .collect(Collectors.joining("', '")) + "'")
                    .build()
                );
                nop = true;
            }
            else
            {
                if (alreadyPlaced.size() < selectFilter.getPlaceCount())
                {
                    errorReporter.logDebug(
                        "Overriding placeCount. From %d to %d",
                        selectFilter.getPlaceCount(),
                        selectFilter.getPlaceCount() - alreadyPlaced.size()
                    );
                    selectorCfg.overridePlaceCount(selectFilter.getPlaceCount() - alreadyPlaced.size());
                    if (selectFilter.getStorPoolNameStr() == null)
                    {
                        errorReporter.logDebug("Setting storPoolName. Using '%s'", selectedStorPoolName);
                        selectorCfg.overrideStorPoolNameStr(selectedStorPoolName);
                    }
                }
                else
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_PLACE_COUNT,
                        String.format(
                            "The resource '%s' was already deployed on %d nodes: %s. " +
                                "Did you mean to autoplace with %d replica?",
                            rscNameStr,
                            alreadyPlaced.size(),
                            alreadyPlaced.stream().map(rsc -> rsc.getAssignedNode().getName().displayValue)
                                .collect(Collectors.joining("', '")),
                            alreadyPlaced.size() + selectFilter.getPlaceCount()
                        )
                    ));
                }

                /*
                 *  here we have two options. Either the user defined a storage pool manually OR the user did
                 *  not specify a storPoolName in which case we set it to "selectedStorPoolName". This means,
                 *  in both cases the selectorCfg.getStorPoolNameStr() will return != null.
                 *
                 *  Additionally restricting the candidates by rscName, will either not change anything at all
                 *  (that is the case when the user set a storage pool manually != selectedStorPoolName), or
                 *  will reduce the candidates on node-level, filtering the nodes out where the resource
                 *  is already deployed.
                 */
                selectorCfg.getNotPlaceWithRscList().add(rscNameStr);
            }
        }

        Flux<ApiCallRc> deploymentResponses;
        if (nop)
        {
            deploymentResponses = Flux.empty();
        }
        else
        {
            Candidate bestCandidate = autoStorPoolSelector.findBestCandidate(
                calculateResourceDefinitionSize(rscNameStr),
                selectorCfg,
                CtrlAutoStorPoolSelector::mostRemainingSpaceNodeStrategy,
                CtrlAutoStorPoolSelector::mostRemainingSpaceCandidateStrategy
            );

            Map<String, String> rscPropsMap = new TreeMap<>();
            String selectedStorPoolName = bestCandidate.storPoolName.displayValue;
            rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, selectedStorPoolName);

            ApiCallRcImpl createResourceResponses = new ApiCallRcImpl();

            List<Resource> deployedResources = new ArrayList<>();
            for (Node node : bestCandidate.nodes)
            {
                ResourceData rsc = ctrlRscCrtApiHelper.createResourceDb(
                    node.getName().displayValue,
                    rscNameStr,
                    0L,
                    rscPropsMap,
                    Collections.emptyList(),
                    null
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
                ArrayList<Node> disklessNodeList = new ArrayList<>(nodesMap.values()); // copy
                disklessNodeList.removeAll(bestCandidate.nodes); // remove all selected nodes
                disklessNodeList.removeAll(
                    // remove all nodes the rsc is already deployed
                    alreadyPlaced.stream().map(Resource::getAssignedNode).collect(Collectors.toList())
                );

                // TODO: allow other diskless storage pools
                rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);

                // deploy resource disklessly on remaining nodes
                for (Node disklessNode : disklessNodeList)
                {
                    try
                    {
                        if (disklessNode.getNodeType(apiCtx) == Node.NodeType.SATELLITE) // only deploy on satellites
                        {
                            deployedResources.add(
                                ctrlRscCrtApiHelper.createResourceDb(
                                    disklessNode.getName().displayValue,
                                    rscNameStr,
                                    RscFlags.DISKLESS.flagValue,
                                    rscPropsMap,
                                    Collections.emptyList(),
                                    null
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

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.CREATED,
                    "Resource '" + rscNameStr + "' successfully autoplaced on " +
                        selectFilter.getPlaceCount() + " nodes"
                )
                .setDetails("Used storage pool: '" + bestCandidate.storPoolName.displayValue + "'\n" +
                    "Used nodes: '" + bestCandidate.nodes.stream()
                    .map(node -> node.getName().displayValue)
                    .collect(Collectors.joining("', '")) + "'")
                .build()
            );

            deploymentResponses = deployedResources.isEmpty() ?
                Flux.empty() :
                ctrlRscCrtApiHelper.deployResources(context, deployedResources);
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

    private Stream<Resource> privilegedStreamResources(Node node)
    {
        Stream<Resource> ret;
        try
        {
            ret = node.streamResources(apiCtx);
        }
        catch (AccessDeniedException accDenied)
        {
            throw new ImplementationError("ApiCtx has not enough privileges", accDenied);
        }
        return ret;
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
