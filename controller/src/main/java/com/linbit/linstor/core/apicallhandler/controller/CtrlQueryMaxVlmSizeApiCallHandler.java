package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlQueryMaxVlmSizeApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlQueryMaxVlmSizeHelper qmvsHelper;

    @Inject
    CtrlQueryMaxVlmSizeApiCallHandler(
        ScopeRunner scopeRunnerRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlQueryMaxVlmSizeHelper qmvsHelperRef
    )
    {
        scopeRunner = scopeRunnerRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        lockGuardFactory = lockGuardFactoryRef;
        qmvsHelper = qmvsHelperRef;
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
        Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> flux;
        if (selectFilter.getReplicaCount() == null)
        {
            flux = Flux.error(
                new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_PLACE_COUNT,
                    "Replica count is required for this operation")
                )
            );
        }
        else
        {
            flux = qmvsHelper.queryMaxVlmSize(selectFilter, null, 0, thinFreeCapacities);
        }

        return flux;
    }
}
