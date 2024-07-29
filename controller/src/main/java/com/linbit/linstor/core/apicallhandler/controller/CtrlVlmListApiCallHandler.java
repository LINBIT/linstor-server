package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import com.linbit.linstor.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlVlmListApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final VlmAllocatedFetcher vlmAllocatedFetcher;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final NodeRepository nodeRepository;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final StltConfigAccessor stltCfgAccessor;

    @Inject
    public CtrlVlmListApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        VlmAllocatedFetcher vlmAllocatedFetcherRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        NodeRepository nodeRepositoryRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        StltConfigAccessor stltCfgAccessorRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        vlmAllocatedFetcher = vlmAllocatedFetcherRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        nodeRepository = nodeRepositoryRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        stltCfgAccessor = stltCfgAccessorRef;
    }

    public Flux<ResourceList> listVlms(
        List<String> nodeNames,
        List<String> storPools,
        List<String> resources,
        List<String> propFilters
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
                    lockGuardFactory.buildDeferred(READ, NODES_MAP, RSC_DFN_MAP),
                    () -> Flux.just(
                        assembleList(nodesFilter, storPoolsFilter, resourceFilter, propFilters, vlmAllocatedAnswers)),
                    MDC.getCopyOfContextMap()
                )
            );
    }

    /**
     *
     * @param nodesFilter
     * @param storPoolsFilter
     * @param resourceFilter
     * @param propFilters
     * @param vlmAllocatedAnswers if null an cached result will be returned
     * @return Filtered ResourceList result
     */
    private ResourceList assembleList(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolsFilter,
        Set<ResourceName> resourceFilter,
        List<String> propFilters,
        final @Nullable Map<Volume.Key, VlmAllocatedResult> vlmAllocatedAnswers
    )
    {
        ResourceList rscList = new ResourceList();
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
                                nodesFilter.contains(rsc.getNode().getName()))
                            .collect(toList()))
                        {
                            // prop filter
                            final ReadOnlyProps props = rsc.getProps(peerAccCtx.get());
                            if (props.contains(propFilters))
                            {
                                // create our api object ourselves to filter the volumes by storage pools

                                // build volume list filtered by storage pools (if provided)
                                List<VolumeApi> volumes = new ArrayList<>();
                                List<AbsRscLayerObject<Resource>> storageRscList = LayerUtils
                                    .getChildLayerDataByKind(
                                    rsc.getLayerData(peerAccCtx.get()),
                                    DeviceLayerKind.STORAGE
                                );
                                Iterator<Volume> itVolumes = rsc.iterateVolumes();
                                while (itVolumes.hasNext())
                                {
                                    Volume vlm = itVolumes.next();
                                    boolean addToList = storPoolsFilter.isEmpty();
                                    if (!addToList)
                                    {
                                        VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();
                                        for (AbsRscLayerObject<Resource> storageRsc : storageRscList)
                                        {
                                            if (storPoolsFilter.contains(
                                                storageRsc.getVlmProviderObject(vlmNr).getStorPool().getName())
                                            )
                                            {
                                                addToList = true;
                                                break;
                                            }
                                        }
                                    }
                                    if (addToList)
                                    {
                                        if (vlmAllocatedAnswers != null)
                                        {
                                            VlmAllocatedResult vlmAllocResult = vlmAllocatedAnswers.get(vlm.getKey());
                                            if (vlmAllocResult != null)
                                            {
                                                vlm.clearReports();
                                                vlm.addReports(vlmAllocResult.getApiCallRc());
                                            }
                                        }
                                        volumes.add(vlm.getApiData(
                                            getAllocated(
                                                vlmAllocatedAnswers, vlm),
                                                peerAccCtx.get()
                                            )
                                        );
                                    }
                                }

                                List<ResourceConnectionApi> rscConns = new ArrayList<>();
                                for (ResourceConnection rscConn : rsc.streamAbsResourceConnections(peerAccCtx.get())
                                        .collect(toList()))
                                {
                                    rscConns.add(rscConn.getApiData(peerAccCtx.get()));
                                }

                                if (!volumes.isEmpty())
                                {
                                    EffectivePropertiesPojo propsPojo = rsc.getEffectiveProps(
                                        peerAccCtx.get(),
                                        stltCfgAccessor
                                    );

                                    RscPojo filteredRscVlms = new RscPojo(
                                        rscDfn.getName().getDisplayName(),
                                        rsc.getNode().getName().getDisplayName(),
                                        rsc.getNode().getUuid(),
                                        rscDfn.getApiData(peerAccCtx.get()),
                                        rsc.getUuid(),
                                        rsc.getStateFlags().getFlagsBits(peerAccCtx.get()),
                                        rsc.getProps(peerAccCtx.get()).map(),
                                        volumes,
                                        null,
                                        rscConns,
                                        null,
                                        null,
                                        rsc.getLayerData(peerAccCtx.get()).asPojo(peerAccCtx.get()),
                                        rsc.getCreateTimestamp().orElse(null),
                                        propsPojo
                                    );
                                    rscList.addResource(filteredRscVlms);
                                }
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

        return rscList;
    }

    private long getAllocated(
        final @Nullable Map<Volume.Key, VlmAllocatedResult> vlmAllocatedCapacities,
        Volume vlm
    )
        throws AccessDeniedException
    {
        long allocated = 0L;
        if (vlmAllocatedCapacities != null)
        {
            // first, check if we just queried the data
            VlmAllocatedResult allocatedResult = vlmAllocatedCapacities.get(vlm.getKey());
            if (allocatedResult == null)
            {
                /*
                 * if the vlm was thinly provisioned, we should have found that in the map, or the
                 * satellite of the vlm is not reachable.
                 *
                 * here it would be quite cumbersome to check if the satellite is reachable, so we simply
                 * test if the vlm is thick-provisioned which would mean that the has already set its
                 * allocated size, as that will not change.
                 */
                if (vlm.isAllocatedSizeSet(peerAccCtx.get()))
                {
                    allocated = vlm.getAllocatedSize(peerAccCtx.get());
                }
                // else the satellite is offline, but an appropriate message should already have been
                // generated by our caller method
            }
            else
            {
                if (!allocatedResult.hasErrors())
                {
                    allocated = allocatedResult.getAllocatedSize();
                }
            }
        }
        else
        if (vlm.isAllocatedSizeSet(peerAccCtx.get()))
        {
            allocated = vlm.getAllocatedSize(peerAccCtx.get());
        }

        return allocated;

        /*
        Long allocated = null;
        DeviceProviderKind driverKind = vlm.getStorPools(peerAccCtx.get()).getDeviceProviderKind();
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
        */
    }

    public ResourceList listVlmsCached(
        List<String> nodeNames,
        List<String> storPools,
        List<String> resources,
        List<String> propFilters
    )
    {
        final Set<NodeName> nodesFilter =
            nodeNames.stream().map(LinstorParsingUtils::asNodeName).collect(Collectors.toSet());
        final Set<StorPoolName> storPoolsFilter =
            storPools.stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        final Set<ResourceName> resourceFilter =
            resources.stream().map(LinstorParsingUtils::asRscName).collect(Collectors.toSet());

        try (LockGuard ignored = lockGuardFactory.build(READ, NODES_MAP, RSC_DFN_MAP))
        {
            return assembleList(nodesFilter, storPoolsFilter, resourceFilter, propFilters, null);
        }
    }

    public static String getVlmDescriptionInline(Volume vlm)
    {
        return getVlmDescriptionInline(vlm.getAbsResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescriptionInline(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescriptionInline(
            rsc.getNode().getName().displayValue,
            rsc.getResourceDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "volume '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" + nodeNameStr + "'";
    }

    public static String getVlmDescription(Volume vlm)
    {
        return getVlmDescription(vlm.getAbsResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescription(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescription(
            rsc.getNode().getName().displayValue,
            rsc.getResourceDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescription(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "Volume '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" + nodeNameStr + "'";
    }
}
