package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinitionRepository;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlStorPoolListApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock storPoolDfnMapLock;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<Long> apiCallId;

    @Inject
    public CtrlStorPoolListApiCallHandler(
        ScopeRunner scopeRunnerRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        scopeRunner = scopeRunnerRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        peerAccCtx = peerAccCtxRef;
        apiCallId = apiCallIdRef;
    }

    public Flux<byte[]> listStorPools(List<String> nodeNames, List<String> storPoolNames)
    {
        final Set<StorPoolName> storPoolsFilter =
            storPoolNames.stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        final Set<NodeName> nodesFilter =
            nodeNames.stream().map(LinstorParsingUtils::asNodeName).collect(Collectors.toSet());

        return freeCapacityFetcher.fetchThinFreeSpaceInfo(nodesFilter)
            .flatMapMany(freeCapacityAnswers ->
                scopeRunner.fluxInTransactionlessScope(
                    "Assemble storage pool list",
                    LockGuard.createDeferred(storPoolDfnMapLock.readLock()),
                    () -> assembleList(nodesFilter, storPoolsFilter, freeCapacityAnswers)
                )
            );
    }

    private Flux<byte[]> assembleList(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolsFilter,
        Tuple2<Map<StorPool.Key, SpaceInfo>, List<ApiCallRc>> freeCapacityAnswers
    )
    {
        ArrayList<StorPool.StorPoolApi> storPools = new ArrayList<>();
        final Map<StorPool.Key, SpaceInfo> freeCapacityMap = freeCapacityAnswers.getT1();
        try
        {
            storPoolDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(storPoolDfn ->
                    (
                        storPoolsFilter.isEmpty() ||
                        storPoolsFilter.contains(storPoolDfn.getName())
                    ) &&
                    !LinStor.DISKLESS_STOR_POOL_NAME.equalsIgnoreCase(storPoolDfn.getName().value)
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
                                Long freeCapacity;
                                Long totalCapacity;

                                SpaceInfo spaceInfo = freeCapacityMap.get(new StorPool.Key(storPool));
                                if (!storPool.getNode().getPeer(peerAccCtx.get()).isConnected())
                                {
                                    freeCapacity = null;
                                    totalCapacity = null;
                                }
                                else if (spaceInfo == null)
                                {
                                    freeCapacity = storPool.getFreeSpaceTracker()
                                        .getFreeCapacityLastUpdated(peerAccCtx.get()).orElse(null);
                                    totalCapacity = storPool.getFreeSpaceTracker()
                                        .getTotalCapacity(peerAccCtx.get()).orElse(null);
                                }
                                else
                                {
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

        Flux<byte[]> flux =  Flux.just(
            clientComSerializer
            .answerBuilder(ApiConsts.API_LST_STOR_POOL, apiCallId.get())
            .storPoolList(storPools)
            .build()
        );

        for (ApiCallRc apiCallRc : freeCapacityAnswers.getT2())
        {
            flux = flux.concatWith(Flux.just(clientComSerializer
                .answerBuilder(ApiConsts.API_REPLY, apiCallId.get())
                .apiCallRcSeries(apiCallRc)
                .build())
            );
        }

        return flux;
    }
}
