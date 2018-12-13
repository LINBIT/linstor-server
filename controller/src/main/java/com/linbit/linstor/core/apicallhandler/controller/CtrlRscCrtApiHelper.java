package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeIdAlloc;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.SwordfishTargetDriverKind;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.ApiUtils.execPriveleged;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Singleton
public class CtrlRscCrtApiHelper
{
    private final AccessContext apiCtx;
    private final Props stltConf;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final EventWaiter eventWaiter;
    private final ResourceStateEvent resourceStateEvent;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDataFactory resourceDataFactory;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlRscCrtApiHelper(
        @ApiContext AccessContext apiCtxRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        EventWaiter eventWaiterRef,
        ResourceStateEvent resourceStateEventRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDataFactory resourceDataFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        stltConf = stltConfRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        eventWaiter = eventWaiterRef;
        resourceStateEvent = resourceStateEventRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceDataFactory = resourceDataFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    /**
     * This method really creates the resource and its volumes.
     *
     * This method does NOT:
     * * commit any transaction
     * * update satellites
     * * create success-apiCallRc entries (only error RC in case of exception)
     *
     * @param nodeNameStr
     * @param rscNameStr
     * @param flags
     * @param rscPropsMap
     * @param vlmApiList
     * @param nodeIdInt
     *
     * @param thinFreeCapacities
     * @return the newly created resource
     */
    public ApiCallRcWith<ResourceData> createResourceDb(
        String nodeNameStr,
        String rscNameStr,
        long flags,
        Map<String, String> rscPropsMap,
        List<? extends Volume.VlmApi> vlmApiList,
        Integer nodeIdInt,
        Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        NodeData node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

        NodeId nodeId = resolveNodeId(nodeIdInt, rscDfn);

        ResourceData rsc = createResource(rscDfn, node, nodeId, flags);
        Props rscProps = ctrlPropsHelper.getProps(rsc);

        ctrlPropsHelper.fillProperties(LinStorObject.RESOURCE, rscPropsMap, rscProps, ApiConsts.FAIL_ACC_DENIED_RSC);

        if (ctrlVlmCrtApiHelper.isDiskless(rsc) && rscPropsMap.get(ApiConsts.KEY_STOR_POOL_NAME) == null)
        {
            rscProps.map().put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);
        }

        boolean hasAlreadySwordfishTargetVolume = execPriveleged(() -> rscDfn.streamResource(apiCtx))
            .flatMap(tmpRsc -> tmpRsc.streamVolumes())
            .anyMatch(vlm ->
                execPriveleged(() -> vlm.getStorPool(apiCtx)).getDriverKind() instanceof SwordfishTargetDriverKind
            );

        List<VolumeData> createdVolumes = new ArrayList<>();

        for (Volume.VlmApi vlmApi : vlmApiList)
        {
            VolumeDefinitionData vlmDfn = loadVlmDfn(rscDfn, vlmApi.getVlmNr(), true);

            VolumeData vlmData = ctrlVlmCrtApiHelper.createVolumeResolvingStorPool(
                rsc,
                vlmDfn,
                thinFreeCapacities,
                vlmApi.getBlockDevice(),
                vlmApi.getMetaDisk()
            ).extractApiCallRc(responses);
            createdVolumes.add(vlmData);

            Props vlmProps = ctrlPropsHelper.getProps(vlmData);

            ctrlPropsHelper.fillProperties(
                LinStorObject.VOLUME, vlmApi.getVlmProps(), vlmProps, ApiConsts.FAIL_ACC_DENIED_VLM);
        }

        Iterator<VolumeDefinition> iterateVolumeDfn = getVlmDfnIterator(rscDfn);
        while (iterateVolumeDfn.hasNext())
        {
            VolumeDefinition vlmDfn = iterateVolumeDfn.next();

            // first check if we probably just deployed a vlm for this vlmDfn
            if (rsc.getVolume(vlmDfn.getVolumeNumber()) == null)
            {
                // not deployed yet.

                VolumeData vlm = ctrlVlmCrtApiHelper.createVolumeResolvingStorPool(
                    rsc,
                    vlmDfn,
                    thinFreeCapacities
                ).extractApiCallRc(responses);
                createdVolumes.add(vlm);
            }
        }

        boolean createsNewSwordfishTargetVolume = createdVolumes.stream()
            .anyMatch(vlm ->
                execPriveleged(() -> vlm.getStorPool(apiCtx)).getDriverKind() instanceof SwordfishTargetDriverKind
            );

        if (createsNewSwordfishTargetVolume && hasAlreadySwordfishTargetVolume)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SWORDFISH_TARGET_PER_RSC_DFN,
                    "Linstor currently only allows one swordfish target per resource definition"
                )
            );
        }

        return new ApiCallRcWith<>(responses, rsc);
    }

    /**
     * Deploy at least one resource of a resource definition to the satellites and wait for them to be ready.
     */
    public Flux<ApiCallRc> deployResources(ResponseContext context, List<Resource> deployedResources)
    {
        long rscDfnCount = deployedResources.stream()
            .map(Resource::getDefinition)
            .map(ResourceDefinition::getName)
            .distinct()
            .count();
        if (rscDfnCount != 1)
        {
            throw new IllegalArgumentException("Resources belonging to precisely one resource definition expected");
        }

        ResourceDefinition rscDfn = deployedResources.get(0).getDefinition();
        ResourceName rscName = rscDfn.getName();

        Set<NodeName> nodeNames = deployedResources.stream()
            .map(Resource::getAssignedNode)
            .map(Node::getName)
            .collect(Collectors.toSet());

        String nodeNamesStr = nodeNames.stream()
            .map(NodeName::getDisplayName)
            .map(displayName -> "''" + displayName + "''")
            .collect(Collectors.joining(", "));

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn)
            .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                updateResponses,
                nodeNames,
                "Created resource {1} on {0}",
                "Added peer(s) " + nodeNamesStr + " to resource {1} on {0}"
            ));

        Publisher<ApiCallRc> readyResponses;
        if (getVolumeDfnCountPriveleged(rscDfn) == 0)
        {
            // No DRBD resource is created when no volumes are present, so do not wait for it to be ready
            readyResponses = Mono.just(responseConverter.addContextAll(
                makeNoVolumesMessage(rscName), context, false));
        }
        else if (allDiskless(rscDfn))
        {
            readyResponses = Mono.just(makeAllDisklessMessage(rscName));
        }
        else
        {
            List<Mono<ApiCallRc>> resourceReadyResponses = new ArrayList<>();
            for (Resource rsc : deployedResources)
            {
                NodeName nodeName = rsc.getAssignedNode().getName();
                if (supportsDrbd(rsc))
                {
                    resourceReadyResponses.add(eventWaiter
                        .waitForStream(
                            resourceStateEvent.get(),
                            ObjectIdentifier.resource(nodeName, rscName)
                        )
                        .skipUntil(UsageState::getResourceReady)
                        .next()
                        .thenReturn(makeResourceReadyMessage(context, nodeName, rscName))
                        .onErrorResume(PeerNotConnectedException.class, ignored -> Mono.just(
                            ApiCallRcImpl.singletonApiCallRc(ResponseUtils.makeNotConnectedWarning(nodeName))
                        ))
                    );
                }
            }
            readyResponses = Flux.merge(resourceReadyResponses);
        }

        return satelliteUpdateResponses.concatWith(readyResponses);
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

    private NodeId getNextFreeNodeId(ResourceDefinitionData rscDfn)
    {
        NodeId freeNodeId;
        try
        {
            Iterator<Resource> rscIterator = rscDfn.iterateResource(peerAccCtx.get());
            int[] occupiedIds = new int[rscDfn.getResourceCount()];
            int idx = 0;
            while (rscIterator.hasNext())
            {
                occupiedIds[idx] = rscIterator.next().getNodeId().value;
                ++idx;
            }
            Arrays.sort(occupiedIds);

            freeNodeId = NodeIdAlloc.getFreeNodeId(occupiedIds);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "iterate the resources of resource definition '" + rscDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (ExhaustedPoolException exhaustedPoolExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_POOL_EXHAUSTED_NODE_ID,
                "An exception occured during generation of a node ID."
            ), exhaustedPoolExc);
        }
        return freeNodeId;
    }

    ResourceData createResource(
        ResourceDefinitionData rscDfn,
        Node node,
        NodeId nodeId,
        long flags
    )
    {
        ResourceData rsc;
        try
        {
            checkPeerSlotsForNewPeer(rscDfn);
            short peerSlots = getAndCheckPeerSlotsForNewResource(rscDfn);

            rsc = resourceDataFactory.create(
                peerAccCtx.get(),
                rscDfn,
                node,
                nodeId,
                Resource.RscFlags.restoreFlags(flags)
            );

            rsc.getProps(peerAccCtx.get()).setProp(ApiConsts.KEY_PEER_SLOTS, Short.toString(peerSlots));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register the " + getRscDescriptionInline(node, rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_RSC,
                "A " + getRscDescriptionInline(node, rscDfn) + " already exists."
            ), dataAlreadyExistsExc);
        }
        catch (InvalidValueException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return rsc;
    }

    private NodeId resolveNodeId(Integer nodeIdInt, ResourceDefinitionData rscDfn)
    {
        NodeId nodeId;

        if (nodeIdInt == null)
        {
            nodeId = getNextFreeNodeId(rscDfn);
        }
        else
        {
            try
            {
                NodeId requestedNodeId = new NodeId(nodeIdInt);

                Iterator<Resource> rscIterator = rscDfn.iterateResource(peerAccCtx.get());
                while (rscIterator.hasNext())
                {
                    if (requestedNodeId.equals(rscIterator.next().getNodeId()))
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_NODE_ID,
                            "The specified node ID is already in use."
                        ));
                    }
                }

                nodeId = requestedNodeId;
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ApiAccessDeniedException(
                    accDeniedExc,
                    "iterate the resources of resource definition '" + rscDfn.getName().displayValue + "'",
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                );
            }
            catch (ValueOutOfRangeException outOfRangeExc)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_NODE_ID,
                    "The specified node ID is out of range."
                ), outOfRangeExc);
            }
        }

        return nodeId;
    }

    private VolumeDefinitionData loadVlmDfn(
        ResourceDefinitionData rscDfn,
        int vlmNr,
        boolean failIfNull
    )
    {
        return loadVlmDfn(rscDfn, LinstorParsingUtils.asVlmNr(vlmNr), failIfNull);
    }

    private VolumeDefinitionData loadVlmDfn(
        ResourceDefinitionData rscDfn,
        VolumeNumber vlmNr,
        boolean failIfNull
    )
    {
        VolumeDefinitionData vlmDfn;
        try
        {
            vlmDfn = (VolumeDefinitionData) rscDfn.getVolumeDfn(peerAccCtx.get(), vlmNr);

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

    private void checkPeerSlotsForNewPeer(ResourceDefinitionData rscDfn)
        throws AccessDeniedException, InvalidKeyException
    {
        int resourceCount = rscDfn.getResourceCount();
        Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIter.hasNext())
        {
            Resource otherRsc = rscIter.next();

            String peerSlotsProp = otherRsc.getProps(peerAccCtx.get()).getProp(ApiConsts.KEY_PEER_SLOTS);
            short peerSlots = peerSlotsProp == null ?
                InternalApiConsts.DEFAULT_PEER_SLOTS :
                Short.valueOf(peerSlotsProp);

            if (peerSlots < resourceCount)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                    "Resource on node " + otherRsc.getAssignedNode().getName().displayValue +
                        " has insufficient peer slots to add another peer"
                ));
            }
        }
    }

    private short getAndCheckPeerSlotsForNewResource(ResourceDefinitionData rscDfn)
        throws InvalidKeyException, AccessDeniedException
    {
        int resourceCount = rscDfn.getResourceCount();

        String peerSlotsNewResourceProp = new PriorityProps(rscDfn.getProps(peerAccCtx.get()), stltConf)
            .getProp(ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE);
        short peerSlots = peerSlotsNewResourceProp == null ?
            InternalApiConsts.DEFAULT_PEER_SLOTS :
            Short.valueOf(peerSlotsNewResourceProp);

        if (peerSlots < resourceCount)
        {
            String detailsMsg = (peerSlotsNewResourceProp == null ? "Default" : "Configured") +
                " peer slot count " + peerSlots + " too low";
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS, "Insufficient peer slots to create resource")
                .setDetails(detailsMsg)
                .setCorrection("Configure a higher peer slot count on the resource definition or controller")
                .build()
            );
        }
        return peerSlots;
    }

    Iterator<VolumeDefinition> getVlmDfnIterator(ResourceDefinitionData rscDfn)
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

    private int getVolumeDfnCountPriveleged(ResourceDefinition rscDfn)
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
            Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
            while (rscIter.hasNext())
            {
                if (!rscIter.next().getStateFlags().isSet(peerAccCtx.get(), Resource.RscFlags.DISKLESS))
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

    private boolean supportsDrbd(Resource rsc)
    {
        boolean supportsDrbd;
        try
        {
            supportsDrbd = rsc.supportsDrbd(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check DRBD support of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return supportsDrbd;
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
            "Resource '" + rscName + "' unusable because it is diskless on all its nodes"
        ));
    }
}
