package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlVlmApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final VlmAllocatedFetcher vlmAllocatedFetcher;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final NodeRepository nodeRepository;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final ResponseConverter responseConverter;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlVlmApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        VlmAllocatedFetcher vlmAllocatedFetcherRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        vlmAllocatedFetcher = vlmAllocatedFetcherRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        nodeRepository = nodeRepositoryRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc modifyVolume(
        UUID vlmUuid,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNrInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespacesRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr,
            vlmNrInt
        );

        try
        {
            VolumeData vlm = ctrlApiDataLoader.loadVlm(nodeNameStr, rscNameStr, vlmNrInt, true);

            if (vlmUuid != null && !vlmUuid.equals(vlm.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_VLM,
                    "UUID-check failed"
                ));
            }

            Props props = ctrlPropsHelper.getProps(vlm);

            ctrlPropsHelper.fillProperties(LinStorObject.VOLUME, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_VLM);
            ctrlPropsHelper.remove(props, deletePropKeys, deletePropNamespacesRef);

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(
                responses,
                context,
                ctrlSatelliteUpdater.updateSatellites(vlm.getResource())
            );
            responseConverter.addWithOp(
                responses,
                context,
                ApiSuccessUtils.defaultModifiedEntry(vlm.getUuid(), getVlmDescriptionInline(vlm))
            );
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public Flux<ApiCallRcWith<ResourceList>> listVlms(
        List<String> nodeNames,
        List<String> storPools,
        List<String> resources
    )
    {
        final Set<NodeName> nodesFilter =
            nodeNames.stream().map(LinstorParsingUtils::asNodeName).collect(Collectors.toSet());
        final Set<StorPoolName> storPoolsFilter =
            storPools.stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        final Set<ResourceName> resourceFilter =
            resources.stream().map(LinstorParsingUtils::asRscName).collect(Collectors.toSet());

        return vlmAllocatedFetcher.fetchVlmAllocated(nodesFilter, storPoolsFilter, resourceFilter)
            .flatMapMany(vlmAllocatedAnswers ->
                scopeRunner.fluxInTransactionlessScope(
                    "Assemble volume list",
                    lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                    () -> assembleList(nodesFilter, storPoolsFilter, resourceFilter, vlmAllocatedAnswers)
                )
            );
    }

    public Flux<ApiCallRcWith<ResourceList>> assembleList(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolsFilter,
        Set<ResourceName> resourceFilter,
        Tuple2<Map<Volume.Key, Long>, List<ApiCallRc>> vlmAllocatedAnswers
    )
    {
        ResourceList rscList = new ResourceList();
        final Map<Volume.Key, Long> vlmAllocatedCapacities = vlmAllocatedAnswers.getT1();
        try
        {
            resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(rscDfn -> resourceFilter.isEmpty() || resourceFilter.contains(rscDfn.getName()))
                .forEach(rscDfn ->
                {
                    try
                    {
                        for (Resource rsc : rscDfn.streamResource(peerAccCtx.get())
                            .filter(rsc -> nodesFilter.isEmpty() ||
                                nodesFilter.contains(rsc.getAssignedNode().getName()))
                            .collect(toList()))
                        {
                            // create our api object ourselves to filter the volumes by storage pools

                            // build volume list filtered by storage pools (if provided)
                            List<Volume.VlmApi> volumes = new ArrayList<>();
                            Iterator<Volume> itVolumes = rsc.iterateVolumes();
                            while (itVolumes.hasNext())
                            {
                                Volume vlm = itVolumes.next();
                                if (storPoolsFilter.isEmpty() ||
                                    storPoolsFilter.contains(vlm.getStorPool(peerAccCtx.get()).getName()))
                                {
                                    volumes.add(vlm.getApiData(
                                        getAllocated(vlmAllocatedCapacities, vlm),
                                        peerAccCtx.get()
                                    ));
                                }
                            }

                            List<ResourceConnection.RscConnApi> rscConns = new ArrayList<>();
                            for (ResourceConnection rscConn : rsc.streamResourceConnections(peerAccCtx.get())
                                    .collect(toList()))
                            {
                                rscConns.add(rscConn.getApiData(peerAccCtx.get()));
                            }

                            if (!volumes.isEmpty())
                            {
                                RscPojo filteredRscVlms = new RscPojo(
                                    rscDfn.getName().getDisplayName(),
                                    rsc.getAssignedNode().getName().getDisplayName(),
                                    rsc.getAssignedNode().getUuid(),
                                    rscDfn.getApiData(peerAccCtx.get()),
                                    rsc.getUuid(),
                                    rsc.getStateFlags().getFlagsBits(peerAccCtx.get()),
                                    rsc.getProps(peerAccCtx.get()).map(),
                                    volumes,
                                    null,
                                    rscConns,
                                    null,
                                    null,
                                    rsc.getLayerData(peerAccCtx.get()).asPojo(peerAccCtx.get())
                                );
                                rscList.addResource(filteredRscVlms);
                            }
                        }
                    }
                    catch (AccessDeniedException accDeniedExc)
                    {
                        // don't add rsc without access
                    }
                }
                );

            // get resource states of all nodes
            for (final Node node : nodeRepository.getMapForView(peerAccCtx.get()).values())
            {
                final Peer satellite = node.getPeer(peerAccCtx.get());
                if (satellite != null)
                {
                    Lock readLock = satellite.getSatelliteStateLock().readLock();
                    readLock.lock();
                    try
                    {
                        final SatelliteState satelliteState = satellite.getSatelliteState();

                        if (satelliteState != null)
                        {
                            rscList.putSatelliteState(node.getName(), new SatelliteState(satelliteState));
                        }
                    }
                    finally
                    {
                        readLock.unlock();
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
            errorReporter.reportError(accDeniedExc);
        }

        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        for (ApiCallRc apiCallRc : vlmAllocatedAnswers.getT2())
        {
            apiCallRcs.addEntries(apiCallRc);
        }

        return Flux.just(new ApiCallRcWith<>(apiCallRcs, rscList));
    }

    private Long getAllocated(Map<Volume.Key, Long> vlmAllocatedCapacities, Volume vlm)
        throws AccessDeniedException
    {
        Long allocated;
        DeviceProviderKind driverKind = vlm.getStorPool(peerAccCtx.get()).getDeviceProviderKind();
        if (driverKind.hasBackingDevice())
        {
            allocated = getDiskAllocated(vlmAllocatedCapacities, vlm);
        }
        else
        {
            // Report the maximum usage of the peer volumes for diskless volumes
            Long maxAllocated = null;
            Iterator<Volume> vlmIter = vlm.getVolumeDefinition().iterateVolumes(peerAccCtx.get());
            while (vlmIter.hasNext())
            {
                Volume peerVlm = vlmIter.next();
                Long peerAllocated = getDiskAllocated(vlmAllocatedCapacities, peerVlm);
                if (peerAllocated != null && (maxAllocated == null || peerAllocated > maxAllocated))
                {
                    maxAllocated = peerAllocated;
                }
            }
            allocated = maxAllocated;
        }
        return allocated;
    }

    private Long getDiskAllocated(Map<Volume.Key, Long> vlmAllocatedCapacities, Volume vlm)
        throws AccessDeniedException
    {
        Long allocated;
        Long fetchedAllocated = vlmAllocatedCapacities.get(vlm.getKey());
        if (fetchedAllocated != null)
        {
            allocated = fetchedAllocated;
        }
        else
        {
            DeviceProviderKind driverKind = vlm.getStorPool(peerAccCtx.get()).getDeviceProviderKind();
            if (driverKind.usesThinProvisioning() || !driverKind.hasBackingDevice())
            {
                allocated = null;
            }
            else
            {
                allocated = vlm.getVolumeDefinition().getVolumeSize(peerAccCtx.get());
            }
        }
        return allocated;
    }

    public static String getVlmDescription(Volume vlm)
    {
        return getVlmDescription(vlm.getResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescription(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescription(
            rsc.getAssignedNode().getName().displayValue,
            rsc.getDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescription(NodeName nodeName, ResourceName rscName, VolumeNumber vlmNr)
    {
        return getVlmDescription(nodeName.getDisplayName(), rscName.displayValue, vlmNr.value);
    }

    public static String getVlmDescription(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "Volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    public static String getVlmDescriptionInline(Volume vlm)
    {
        return getVlmDescriptionInline(vlm.getResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescriptionInline(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescriptionInline(
            rsc.getAssignedNode().getName().displayValue,
            rsc.getDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    static ResponseContext makeVlmContext(
        ApiOperation operation,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));

        return new ResponseContext(
            operation,
            getVlmDescription(nodeNameStr, rscNameStr, vlmNr),
            getVlmDescriptionInline(nodeNameStr, rscNameStr, vlmNr),
            ApiConsts.MASK_VLM,
            objRefs
        );
    }
}
