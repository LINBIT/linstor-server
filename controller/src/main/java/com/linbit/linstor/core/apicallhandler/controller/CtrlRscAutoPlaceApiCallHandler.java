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
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class CtrlRscAutoPlaceApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlAutoStorPoolSelector autoStorPoolSelector;
    private final CtrlRscApiCallHandler rscApiCallHandler;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodesMap nodesMap;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscAutoPlaceApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlAutoStorPoolSelector autoStorPoolSelectorRef,
        CtrlRscApiCallHandler rscApiCallHandlerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodesMap nodesMapRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        autoStorPoolSelector = autoStorPoolSelectorRef;
        rscApiCallHandler = rscApiCallHandlerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodesMap = nodesMapRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc autoPlace(
        String rscNameStr,
        AutoSelectFilterApi selectFilter,
        boolean disklessOnRemainingNodes
    )
    {
        // TODO extract this method into an own interface implementation
        // that the controller can choose between different auto-place strategies

        ApiCallRcImpl responses = new ApiCallRcImpl();

        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        ResponseContext context = new ResponseContext(
            peer.get(),
            ApiOperation.makeRegisterOperation(),
            getObjectDescription(rscNameStr),
            getObjectDescriptionInline(rscNameStr),
            ApiConsts.MASK_RSC,
            objRefs
        );

        try
        {
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
                    alreadyPlaced.get(0).getProps(peerAccCtx.get())
                        .getProp(InternalApiConsts.RSC_PROP_KEY_AUTO_SELECTED_STOR_POOL_NAME);
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

            if (!nop)
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

                List<ResourceData> deployedResources = new ArrayList<>();
                for (Node node : bestCandidate.nodes)
                {
                    ResourceData rsc = rscApiCallHandler.createResource0(
                        node.getName().displayValue,
                        rscNameStr,
                        Collections.emptyList(),
                        rscPropsMap,
                        Collections.emptyList(),
                        null
                    ).extractApiCallRc(responses);
                    deployedResources.add(rsc);

                    // bypass the whilteList
                    rsc.getProps(apiCtx).setProp(
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

                    List<String> flagList = new ArrayList<>();
                    flagList.add(RscFlags.DISKLESS.name());

                    // deploy resource disklessly on remaining nodes
                    for (Node disklessNode : disklessNodeList)
                    {
                        deployedResources.add(
                            rscApiCallHandler.createResource0(
                                disklessNode.getName().displayValue,
                                rscNameStr,
                                flagList,
                                rscPropsMap,
                                Collections.emptyList(),
                                null
                            ).extractApiCallRc(responses)
                        );
                    }
                }

                ctrlTransactionHelper.commit();

                if (!deployedResources.isEmpty())
                {
                    responseConverter.addWithDetail(responses, context,
                        ctrlSatelliteUpdater.updateSatellites(deployedResources.get(0).getDefinition()));
                }

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
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
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
