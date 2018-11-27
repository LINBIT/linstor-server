package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.GenericName;
import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.utils.ComparatorUtils;
import reactor.core.publisher.Flux;

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
            .flatMapMany(thinFreeCapacities -> scopeRunner
                .fluxInTransactionlessScope(
                    "Query max volume size",
                    LockGuard.createDeferred(
                        nodesMapLock.writeLock(),
                        storPoolDfnMapLock.writeLock()
                    ),
                    () -> queryMaxVlmSizeInScope(selectFilter, thinFreeCapacities)
                ));
    }

    private Flux<byte[]> queryMaxVlmSizeInScope(
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
            selectFilter.getStorPoolNameStr()
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

    private byte[] makeResponse(List<MaxVlmSizeCandidatePojo> candidates)
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
                MaxVlmSizeCandidatePojo::getStorPoolDfnApi,
                Comparator.comparing(StorPoolDefinition.StorPoolDfnApi::getName)
            ));

            result = clientComSerializer
                .answerBuilder(ApiConsts.API_RSP_MAX_VLM_SIZE, apiCallIdProvider.get())
                .maxVlmSizeCandidateList(candidates)
                .build();
        }
        return result;
    }
}
