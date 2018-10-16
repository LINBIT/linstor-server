package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.linstor.StorPool;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.security.AccessContext;
import com.linbit.locks.LockGuard;
import com.linbit.utils.ComparatorUtils;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class CtrlQueryMaxVlmSizeApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlAutoStorPoolSelector ctrlAutoStorPoolSelector;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final CtrlClientSerializer clientComSerializer;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final Provider<Long> apiCallIdProvider;

    @Inject
    CtrlQueryMaxVlmSizeApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlAutoStorPoolSelector ctrlAutoStorPoolSelectorRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        CtrlClientSerializer clientComSerializerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlAutoStorPoolSelector = ctrlAutoStorPoolSelectorRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        clientComSerializer = clientComSerializerRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        apiCallIdProvider = apiCallIdProviderRef;
    }

    public Flux<byte[]> queryMaxVlmSize(AutoSelectFilterApi selectFilter)
    {
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
            .flatMapMany(freeCapacities -> scopeRunner
                .fluxInTransactionlessScope(
                    LockGuard.createDeferred(
                        nodesMapLock.writeLock(),
                        storPoolDfnMapLock.writeLock()
                    ),
                    () -> queryMaxVlmSizeInScope(selectFilter, freeCapacities)
                ));
    }

    private Flux<byte[]> queryMaxVlmSizeInScope(
        AutoSelectFilterApi selectFilter,
        Map<StorPool.Key, Long> freeCapacities
    )
    {
        AutoStorPoolSelectorConfig autoStorPoolSelectorConfig = new AutoStorPoolSelectorConfig(
            selectFilter.getPlaceCount(),
            selectFilter.getReplicasOnDifferentList(),
            selectFilter.getReplicasOnSameList(),
            selectFilter.getNotPlaceWithRscRegex(),
            selectFilter.getNotPlaceWithRscList(),
            selectFilter.getStorPoolNameStr()
        );

        List<Candidate> candidates = ctrlAutoStorPoolSelector.getCandidateList(
            ctrlAutoStorPoolSelector.listAvailableStorPools(),
            autoStorPoolSelectorConfig,
            FreeCapacityAutoPoolSelectorUtils.mostFreeCapacityNodeStrategy(freeCapacities)
        );

        List<Tuple2<Candidate, Long>> candidatesWithCapacity = candidates.stream()
            .flatMap(candidate -> candidateWithCapacity(freeCapacities, candidate))
            .collect(Collectors.toList());

        return Flux.just(makeResponse(candidatesWithCapacity));
    }

    private Stream<Tuple2<Candidate, Long>> candidateWithCapacity(
        Map<StorPool.Key, Long> freeCapacities,
        Candidate candidate
    )
    {
        Optional<Long> freeCapacity = FreeCapacityAutoPoolSelectorUtils.getFreeCapacityCurrentEstimationPrivileged(
            apiCtx,
            freeCapacities,
            FreeCapacityAutoPoolSelectorUtils.getCandidateStorPoolPrivileged(apiCtx, candidate)
        );
        return freeCapacity.map(capacity -> Stream.of(Tuples.of(candidate, capacity))).orElseGet(Stream::empty);
    }

    private byte[] makeResponse(List<Tuple2<Candidate, Long>> candidates)
    {
        byte[] result;
        if (candidates.isEmpty())
        {
            result = clientComSerializer
                .answerBuilder(ApiConsts.API_REPLY, apiCallIdProvider.get())
                .apiCallRcSeries(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
                    ApiConsts.MASK_ERROR | ApiConsts.FAIL_NOT_ENOUGH_NODES,
                    "Not enough nodes"
                )))
                .build();
        }
        else
        {
            candidates.sort(ComparatorUtils.comparingWithComparator(
                Tuple2::getT1,
                Comparator.comparing(Candidate::getStorPoolName)
            ));

            result = clientComSerializer
                .answerBuilder(ApiConsts.API_RSP_MAX_VLM_SIZE, apiCallIdProvider.get())
                .maxVlmSizeCandidateList(candidates)
                .build();
        }
        return result;
    }
}
