package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.GenericName;
import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.protobuf.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.ComparatorUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlQueryMaxVlmSizeApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlAutoStorPoolSelector ctrlAutoStorPoolSelector;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    CtrlQueryMaxVlmSizeApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlAutoStorPoolSelector ctrlAutoStorPoolSelectorRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlAutoStorPoolSelector = ctrlAutoStorPoolSelectorRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        lockGuardFactory = lockGuardFactoryRef;
    }

    public Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> queryMaxVlmSize(AutoSelectFilterApi selectFilter)
    {
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
            .flatMapMany(thinFreeCapacities -> scopeRunner
                .fluxInTransactionlessScope(
                    "Query max volume size",
                    lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
                    () -> queryMaxVlmSizeInScope(selectFilter, thinFreeCapacities)
                ));
    }

    private Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> queryMaxVlmSizeInScope(
        AutoSelectFilterApi selectFilter,
        Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        AutoStorPoolSelectorConfig autoStorPoolSelectorConfig = new AutoStorPoolSelectorConfig(
            selectFilter.getPlaceCount(),
            selectFilter.getReplicasOnDifferentList(),
            selectFilter.getReplicasOnSameList(),
            selectFilter.getNotPlaceWithRscRegex(),
            selectFilter.getNotPlaceWithRscList(),
            selectFilter.getStorPoolNameStr(),
            selectFilter.getLayerStackList(),
            selectFilter.getProviderList()
        );

        List<Candidate> candidates = ctrlAutoStorPoolSelector.getCandidateList(
            ctrlAutoStorPoolSelector.listAvailableStorPools(),
            autoStorPoolSelectorConfig,
            FreeCapacityAutoPoolSelectorUtils.mostFreeCapacityNodeStrategy(thinFreeCapacities)
        );

        List<MaxVlmSizeCandidatePojo> candidatesWithCapacity = candidates.stream()
            .flatMap(candidate -> candidateWithCapacity(thinFreeCapacities, candidate))
            .collect(Collectors.toList());

        return Flux.just(makeResponse(candidatesWithCapacity));
    }

    private Stream<MaxVlmSizeCandidatePojo> candidateWithCapacity(
        Map<StorPool.Key, Long> thinFreeCapacities,
        Candidate candidate
    )
    {
        StorPool storPool = FreeCapacityAutoPoolSelectorUtils.getCandidateStorPoolPrivileged(apiCtx, candidate);
        Optional<Long> freeCapacity = FreeCapacityAutoPoolSelectorUtils.getFreeCapacityCurrentEstimationPrivileged(
            apiCtx,
            thinFreeCapacities,
            storPool
        );
        return freeCapacity
            .map(capacity -> Stream.of(new MaxVlmSizeCandidatePojo(
                getStorPoolDfnApiData(storPool),
                candidate.allThin(),
                candidate.getNodes().stream()
                    .map(Node::getName)
                    .map(GenericName::getDisplayName)
                    .collect(Collectors.toList()),
                capacity
            )))
            .orElseGet(Stream::empty);
    }

    private StorPoolDefinition.StorPoolDfnApi getStorPoolDfnApiData(StorPool storPool)
    {
        StorPoolDefinition.StorPoolDfnApi apiData;
        try
        {
            apiData = storPool.getDefinition(apiCtx).getApiData(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return apiData;
    }

    private ApiCallRcWith<List<MaxVlmSizeCandidatePojo>> makeResponse(List<MaxVlmSizeCandidatePojo> candidates)
    {
        ApiCallRc apirc = null;
        if (candidates.isEmpty())
        {
            apirc = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_ERROR | ApiConsts.FAIL_NOT_ENOUGH_NODES,
                "Not enough nodes"
            ));
        }
        else
        {
            candidates.sort(ComparatorUtils.comparingWithComparator(
                MaxVlmSizeCandidatePojo::getStorPoolDfnApi,
                Comparator.comparing(StorPoolDefinition.StorPoolDfnApi::getName)
            ));
        }
        return new ApiCallRcWith<>(apirc, candidates);
    }
}
