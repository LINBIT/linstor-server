package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.VolumeApi;
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
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.RegexMatcher;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlVlmListApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final VlmAllocatedFetcher vlmAllocatedFetcher;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final NodeRepository nodeRepository;
    private final LockGuardFactory lockGuardFactory;
    private final StltConfigAccessor stltCfgAccessor;

    @Inject
    public CtrlVlmListApiCallHandler(
        ScopeRunner scopeRunnerRef,
        VlmAllocatedFetcher vlmAllocatedFetcherRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        NodeRepository nodeRepositoryRef,
        LockGuardFactory lockGuardFactoryRef,
        StltConfigAccessor stltCfgAccessorRef
    )
    {
        scopeRunner = scopeRunnerRef;
        vlmAllocatedFetcher = vlmAllocatedFetcherRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        nodeRepository = nodeRepositoryRef;
        lockGuardFactory = lockGuardFactoryRef;
        stltCfgAccessor = stltCfgAccessorRef;
    }

    public Flux<ResourceList> listVlms(
        List<String> nodeNames,
        List<String> storPools,
        List<String> resources,
        List<String> propFilters
    )
    {
        final List<Pattern> nodesFilter = RegexMatcher.compileAll(nodeNames, true);
        final List<Pattern> storPoolsFilter = RegexMatcher.compileAll(storPools, true);
        final List<Pattern> resourceFilter = RegexMatcher.compileAll(resources, true);

        return vlmAllocatedFetcher.fetchVlmAllocated(
                nodesFilter,
                resolveFetchStorPools(storPools),
                resolveFetchResources(resources)
            )
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
     * @param vlmAllocatedAnswers if null an cached result will be returned
     * @return Filtered ResourceList result
     */
    private ResourceList assembleList(
        List<Pattern> nodesFilter,
        List<Pattern> storPoolsFilter,
        List<Pattern> resourceFilter,
        List<String> propFilters,
        final @Nullable Map<Volume.Key, VlmAllocatedResult> vlmAllocatedAnswers
    )
    {
        ResourceList rscList = new ResourceList();
        resourceDefinitionRepository.getMapForView().values().stream()
            .filter(rscDfn -> RegexMatcher.matchesAny(resourceFilter, rscDfn.getName().displayValue))
            .forEach(rscDfn ->
            {
                for (Resource rsc : rscDfn.streamResource()
                    .filter(rsc -> RegexMatcher.matchesAny(
                        nodesFilter, rsc.getNode().getName().displayValue))
                    .collect(toList()))
                {
                    // prop filter
                    final ReadOnlyProps props = rsc.getProps();
                    if (props.contains(propFilters))
                    {
                        // create our api object ourselves to filter the volumes by storage pools

                        // build volume list filtered by storage pools (if provided)
                        List<VolumeApi> volumes = new ArrayList<>();
                        List<AbsRscLayerObject<Resource>> storageRscList = LayerUtils
                            .getChildLayerDataByKind(
                            rsc.getLayerData(),
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
                                    if (RegexMatcher.matchesAny(
                                        storPoolsFilter,
                                        storageRsc.getVlmProviderObject(vlmNr).getStorPool().getName()
                                            .displayValue)
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
                                        vlmAllocatedAnswers, vlm)
                                    )
                                );
                            }
                        }

                        List<ResourceConnectionApi> rscConns = new ArrayList<>();
                        for (ResourceConnection rscConn : rsc.getAbsResourceConnections())
                        {
                            rscConns.add(rscConn.getApiData());
                        }

                        if (!volumes.isEmpty())
                        {
                            EffectivePropertiesPojo propsPojo = rsc.getEffectiveProps(
                                stltCfgAccessor
                            );

                            RscPojo filteredRscVlms = new RscPojo(
                                rscDfn.getName().getDisplayName(),
                                rsc.getNode().getName().getDisplayName(),
                                rsc.getNode().getUuid(),
                                rscDfn.getApiData(),
                                rsc.getUuid(),
                                rsc.getStateFlags().getFlagsBits(),
                                rsc.getProps().map(),
                                volumes,
                                null,
                                rscConns,
                                null,
                                null,
                                rsc.getLayerData().asPojo(),
                                rsc.getCreateTimestamp().orElse(null),
                                propsPojo
                            );
                            rscList.addResource(filteredRscVlms);
                        }
                    }
                }
            }
            );

        // get resource states of all nodes
        for (final Node node : nodeRepository.getMapForView().values())
        {
            final Peer satellite = node.getPeer();
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

        return rscList;
    }

    private long getAllocated(
        final @Nullable Map<Volume.Key, VlmAllocatedResult> vlmAllocatedCapacities,
        Volume vlm
    )
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
                if (vlm.isAllocatedSizeSet())
                {
                    allocated = vlm.getAllocatedSize();
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
        if (vlm.isAllocatedSizeSet())
        {
            allocated = vlm.getAllocatedSize();
        }

        return allocated;

        /*
        Long allocated = null;
        DeviceProviderKind driverKind = vlm.getStorPools().getDeviceProviderKind();
        if (driverKind.hasBackingDevice())
        {
            allocated = getDiskAllocated(vlmAllocatedCapacities, vlm);
        }
        else
        {
            // Report the maximum usage of the peer volumes for diskless volumes
            Long maxAllocated = null;
            Iterator<Volume> vlmIter = vlm.getVolumeDefinition().iterateVolumes();
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
        final List<Pattern> nodesFilter = RegexMatcher.compileAll(nodeNames, true);
        final List<Pattern> storPoolsFilter = RegexMatcher.compileAll(storPools, true);
        final List<Pattern> resourceFilter = RegexMatcher.compileAll(resources, true);

        try (LockGuard ignored = lockGuardFactory.build(READ, NODES_MAP, RSC_DFN_MAP))
        {
            return assembleList(nodesFilter, storPoolsFilter, resourceFilter, propFilters, null);
        }
    }

    /*
     * Node filtering for the fetcher is done by regex (see fetchVlmAllocated(List<Pattern>, ...)), so only
     * matching satellites are queried. The storage-pool and resource filters below are still resolved to
     * exact-name sets: an exact (non-regex) filter narrows the satellite request, while a regex filter
     * resolves to an empty set (= no narrowing) and assembleList applies the regex afterwards.
     */
    private Set<StorPoolName> resolveFetchStorPools(List<String> storPoolNames)
    {
        final Set<StorPoolName> result;
        if (storPoolNames.isEmpty() || storPoolNames.stream().anyMatch(RegexMatcher::isRegex))
        {
            result = new HashSet<>();
        }
        else
        {
            result = storPoolNames.stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        }
        return result;
    }

    private Set<ResourceName> resolveFetchResources(List<String> resourceNames)
    {
        final Set<ResourceName> result;
        if (resourceNames.isEmpty() || resourceNames.stream().anyMatch(RegexMatcher::isRegex))
        {
            result = new HashSet<>();
        }
        else
        {
            result = resourceNames.stream().map(LinstorParsingUtils::asRscName).collect(Collectors.toSet());
        }
        return result;
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
