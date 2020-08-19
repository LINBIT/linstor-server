package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlStorPoolListApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlStorPoolListApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    public Flux<List<StorPoolApi>> listStorPools(
        List<String> nodeNames,
        List<String> storPoolNames,
        List<String> propFilters
    )
    {
        Flux<List<StorPoolApi>> flux;
        final Set<StorPoolName> storPoolsFilter =
            storPoolNames.stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        final Set<NodeName> nodesFilter =
            nodeNames.stream().map(LinstorParsingUtils::asNodeName).collect(Collectors.toSet());

        flux = freeCapacityFetcher.fetchThinFreeSpaceInfo(nodesFilter)
            .flatMapMany(freeCapacityAnswers ->
                scopeRunner.fluxInTransactionlessScope(
                    "Assemble storage pool list",
                    lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.STOR_POOL_DFN_MAP),
                    () -> Flux.just(assembleList(nodesFilter, storPoolsFilter, propFilters, freeCapacityAnswers))
                )
            );

        return flux;
    }

    public List<StorPoolApi> listStorPoolsCached(
        List<String> nodeNames,
        List<String> storPoolNames,
        List<String> propFilters
    )
    {
        final Set<StorPoolName> storPoolsFilter =
            storPoolNames.stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        final Set<NodeName> nodesFilter =
            nodeNames.stream().map(LinstorParsingUtils::asNodeName).collect(Collectors.toSet());

        try (LockGuard ignored = lockGuardFactory.build(READ, STOR_POOL_DFN_MAP))
        {
            return assembleList(nodesFilter, storPoolsFilter, propFilters, null);
        }
    }

    private List<StorPoolApi> assembleList(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolsFilter,
        List<String> propFilters,
        @Nullable Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>> freeCapacityAnswers
    )
    {
        ArrayList<StorPoolApi> storPools = new ArrayList<>();
        try
        {
            storPoolDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(
                    storPoolDfn -> storPoolsFilter.isEmpty() ||
                    storPoolsFilter.contains(storPoolDfn.getName())
                )
                .forEach(storPoolDfn ->
                    {
                        try
                        {
                            for (StorPool storPool : storPoolDfn.streamStorPools(peerAccCtx.get())
                                .filter(storPool -> nodesFilter.isEmpty() ||
                                    nodesFilter.contains(storPool.getNode().getName()))
                                .collect(toList()))
                            {
                                Props props = storPool.getProps(peerAccCtx.get());
                                if (!props.contains(propFilters))
                                    continue;

                                Long freeCapacity;
                                Long totalCapacity;

                                final Tuple2<SpaceInfo, List<ApiCallRc>> storageInfo = freeCapacityAnswers != null ?
                                    freeCapacityAnswers.get(new StorPool.Key(storPool)) : null;

                                Peer peer = storPool.getNode().getPeer(peerAccCtx.get());
                                if (peer == null || !peer.isConnected())
                                {
                                    freeCapacity = null;
                                    totalCapacity = null;
                                    storPool.clearReports();
                                    storPool.addReports(
                                        new ApiCallRcImpl(
                                            ResponseUtils.makeNotConnectedWarning(storPool.getNode().getName())
                                        )
                                    );
                                }
                                else
                                if (storageInfo == null)
                                {
                                    freeCapacity = storPool.getFreeSpaceTracker()
                                        .getFreeCapacityLastUpdated(peerAccCtx.get()).orElse(null);
                                    totalCapacity = storPool.getFreeSpaceTracker()
                                        .getTotalCapacity(peerAccCtx.get()).orElse(null);
                                }
                                else
                                {
                                    SpaceInfo spaceInfo = storageInfo.getT1();
                                    storPool.clearReports();
                                    for (ApiCallRc apiCallRc : storageInfo.getT2())
                                    {
                                        storPool.addReports(apiCallRc);
                                    }

                                    freeCapacity = spaceInfo.freeCapacity;
                                    totalCapacity = spaceInfo.totalCapacity;
                                }

                                // fullSyncId and updateId null, as they are not going to be serialized anyway
                                storPools.add(storPool.getApiData(
                                    totalCapacity,
                                    freeCapacity,
                                    peerAccCtx.get(),
                                    null,
                                    null
                                ));
                            }
                        }
                        catch (AccessDeniedException accDeniedExc)
                        {
                            // don't add storpooldfn without access
                        }
                    }
                );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "view storage pool definitions",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }

        return storPools;
    }
}
