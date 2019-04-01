package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDataControllerFactory;
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
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.ApiUtils.execPrivileged;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
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
    private final ErrorReporter errorReporter;
    private final Props stltConf;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final EventWaiter eventWaiter;
    private final ResourceStateEvent resourceStateEvent;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDataControllerFactory resourceDataFactory;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlRscCrtApiHelper(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        EventWaiter eventWaiterRef,
        ResourceStateEvent resourceStateEventRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDataControllerFactory resourceDataFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;
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
     * @param layerStackStrListRef
     * @return the newly created resource
     */
    public ApiCallRcWith<ResourceData> createResourceDb(
        String nodeNameStr,
        String rscNameStr,
        long flags,
        Map<String, String> rscPropsMap,
        List<? extends Volume.VlmApi> vlmApiList,
        Integer nodeIdInt,
        Map<StorPool.Key, Long> thinFreeCapacities,
        List<String> layerStackStrListRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        NodeData node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

        List<DeviceLayerKind> layerStack = LinstorParsingUtils.asDeviceLayerKind(layerStackStrListRef);

        if (!layerStack.isEmpty() && !layerStack.get(layerStack.size() - 1).equals(DeviceLayerKind.STORAGE))
        {
            layerStack.add(DeviceLayerKind.STORAGE);
            warnAddedStorageLayer(responses);
        }

        ResourceData rsc = createResource(rscDfn, node, nodeIdInt, flags, layerStack);
        Props rscProps = ctrlPropsHelper.getProps(rsc);

        ctrlPropsHelper.fillProperties(LinStorObject.RESOURCE, rscPropsMap, rscProps, ApiConsts.FAIL_ACC_DENIED_RSC);

        if (ctrlVlmCrtApiHelper.isDiskless(rsc) && rscPropsMap.get(ApiConsts.KEY_STOR_POOL_NAME) == null)
        {
            rscProps.map().put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);
        }

        boolean hasAlreadySwordfishTargetVolume = execPrivileged(() -> rscDfn.streamResource(apiCtx))
            .flatMap(tmpRsc -> tmpRsc.streamVolumes())
            .anyMatch(vlm ->
                execPrivileged(
                    () -> DeviceProviderKind.SWORDFISH_TARGET.equals(
                        vlm.getStorPool(apiCtx).getDeviceProviderKind()
                    )
                )
            );

        List<VolumeData> createdVolumes = new ArrayList<>();

        for (Volume.VlmApi vlmApi : vlmApiList)
        {
            VolumeDefinitionData vlmDfn = loadVlmDfn(rscDfn, vlmApi.getVlmNr(), true);

            VolumeData vlmData = ctrlVlmCrtApiHelper.createVolumeResolvingStorPool(
                rsc,
                vlmDfn,
                thinFreeCapacities
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

                setDrbdPropsForThinVolumesIfNeeded(vlm);
            }
        }

        boolean createsNewSwordfishTargetVolume = createdVolumes.stream()
            .anyMatch(vlm ->
                execPrivileged(
                    () -> DeviceProviderKind.SWORDFISH_TARGET.equals(
                        vlm.getStorPool(apiCtx).getDeviceProviderKind()
                    )
                )
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

    private void warnAddedStorageLayer(ApiCallRcImpl responsesRef)
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

    private void setDrbdPropsForThinVolumesIfNeeded(VolumeData vlmRef)
    {
        try
        {
            DeviceProviderKind deviceProviderKind = vlmRef.getStorPool(peerAccCtx.get()).getDeviceProviderKind();
            if (
                deviceProviderKind.usesThinProvisioning() &&
                LayerUtils.hasLayer(vlmRef.getResource().getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD)
            )
            {
                //TODO: make these default drbd-properties configurable (provider-specific?)

                Props props = vlmRef.getVolumeDefinition().getProps(peerAccCtx.get());
                if (props.getProp("rs-discard-granularity", ApiConsts.NAMESPC_DRBD_DISK_OPTIONS) == null)
                {
                    String dflt;
                    if (deviceProviderKind.equals(DeviceProviderKind.ZFS_THIN))
                    {
                        dflt = "8192";
                    }
                    else
                    {
                        dflt = "65536";
                    }
                    props.setProp("rs-discard-granularity", dflt,  ApiConsts.NAMESPC_DRBD_DISK_OPTIONS);
                }
                if (props.getProp("discard-zeroes-if-aligned", ApiConsts.NAMESPC_DRBD_DISK_OPTIONS) == null)
                {
                    props.setProp("discard-zeroes-if-aligned", "yes",  ApiConsts.NAMESPC_DRBD_DISK_OPTIONS);
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_ACC_DENIED_VLM_DFN,
                    "Linstor currently only allows one swordfish target per resource definition"
                )
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError("Invalid hardcoded thin-volume related properties", exc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
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
            .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                updateResponses,
                rscName,
                nodeNames,
                "Created resource {1} on {0}",
                "Added peer(s) " + nodeNamesStr + " to resource {1} on {0}"
            ));

        Publisher<ApiCallRc> readyResponses;
        if (getVolumeDfnCountPrivileged(rscDfn) == 0)
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
                if (containsDrbdLayerData(rsc))
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

    ResourceData createResource(
        ResourceDefinitionData rscDfn,
        Node node,
        Integer nodeIdIntRef,
        long flags,
        List<DeviceLayerKind> layerStackRef
    )
    {
        if (!layerStackRef.isEmpty())
        {
            ensureLayerStackIsAllowed(layerStackRef);
        }

        ResourceData rsc;
        try
        {
            checkPeerSlotsForNewPeer(rscDfn);

            rsc = resourceDataFactory.create(
                peerAccCtx.get(),
                rscDfn,
                node,
                nodeIdIntRef,
                Resource.RscFlags.restoreFlags(flags),
                layerStackRef
            );

            Set<DeviceLayerKind> unsupportedLayers = getUnsupportedLayers(rsc);
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
        return rsc;
    }

    private Set<DeviceLayerKind> getUnsupportedLayers(ResourceData rsc) throws AccessDeniedException
    {
        Set<DeviceLayerKind> usedDeviceLayerKinds = LayerUtils.getUsedDeviceLayerKinds(
            rsc.getLayerData(peerAccCtx.get())
        );
        usedDeviceLayerKinds.removeAll(
            rsc.getAssignedNode()
                .getPeer(peerAccCtx.get())
                .getSupportedLayers()
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
        throws AccessDeniedException
    {
        int resourceCount = 0;
        Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (LayerUtils.hasLayer(rsc.getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD))
            {
                resourceCount++;
            }
        }

        rscIter = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIter.hasNext())
        {
            Resource otherRsc = rscIter.next();

            List<RscLayerObject> drbdRscDataList = LayerUtils.getChildLayerDataByKind(
                otherRsc.getLayerData(peerAccCtx.get()),
                DeviceLayerKind.DRBD
            );

            for (RscLayerObject rscLayerObj : drbdRscDataList)
            {
                if (((DrbdRscData) rscLayerObj).getPeerSlots() < resourceCount)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                            "Resource on node " + otherRsc.getAssignedNode().getName().displayValue +
                            " has insufficient peer slots to add another peer"
                        )
                    );
                }
            }
        }
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

    private boolean containsDrbdLayerData(Resource rsc)
    {
        List<RscLayerObject> drbdLayerData;
        try
        {
            drbdLayerData = LayerUtils.getChildLayerDataByKind(
                rsc.getLayerData(peerAccCtx.get()),
                DeviceLayerKind.DRBD
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "scan layer data for DRBD layer " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return !drbdLayerData.isEmpty();
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
