package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoResponsePojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoResponsePojo;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.StorPoolFilter;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.Key;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;

@Singleton
public class CtrlQuerySizeInfoHelper
{
    private static final int SEC_TO_MS = 1000;

    private final ErrorReporter errorReporter;
    private final AccessContext sysAccCtx;
    private final Provider<AccessContext> peerCtxProvider;
    private final Autoplacer autoplacer;
    private final StorPoolFilter storPoolFilter;

    private final Map<AutoSelectFilterPojo, CacheEntry<QueryAllSizeInfoResponsePojo>> cachedQasiMap;
    private final Map<String /* RgName */, Map<AutoSelectFilterPojo, CacheEntry<ApiCallRcWith<QuerySizeInfoResponsePojo>>>> cachedQsiPojoMap;

    private final SystemConfRepository sysCfgRepo;

    @Inject
    public CtrlQuerySizeInfoHelper(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysAccCtxRef,
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        Autoplacer autoplacerRef,
        StorPoolFilter storPoolFilterRef,
        SystemConfRepository sysCfgRepoRef
    )
    {
        errorReporter = errorReporterRef;
        sysAccCtx = sysAccCtxRef;
        peerCtxProvider = peerCtxProviderRef;
        autoplacer = autoplacerRef;
        storPoolFilter = storPoolFilterRef;
        sysCfgRepo = sysCfgRepoRef;

        // weak hash maps so the garbage-collector is free to cleanup the cache if it feels like it
        cachedQasiMap = new WeakHashMap<>();
        cachedQsiPojoMap = new WeakHashMap<>();
    }

    public QuerySizeInfoResponsePojo queryMaxVlmSize(
        AutoSelectFilterPojo selectCfgRef,
        Map<Key, Long> thinFreeCapacitiesRef
    )
        throws AccessDeniedException
    {
        /*
         * This is not a hack :)
         *
         * The main difference between this method and CtrlQueryMaxVlmSizeHelper's queryMaxVlmSize
         * is that this method does not need to be dumbed down (i.e. let the autoplacer run multiple
         * times in order to match the very old autoplacer's behavior where only one storage pool
         * name could be selected across multiple nodes).
         */

        int placeCount = selectCfgRef.getReplicaCount();

        Set<StorPool> selectedStorPoolSet = autoplacer.autoPlace(
            selectCfgRef,
            null,
            0
        );
        ArrayList<StorPool> availableStorPoolList = storPoolFilter.filter(
            selectCfgRef,
            storPoolFilter.listAvailableStorPools(true),
            null,
            0,
            null
        );

        long maxVlmSize = getMaxVlmSize(selectedStorPoolSet, thinFreeCapacitiesRef);
        long available = getAvailable(placeCount, selectedStorPoolSet, availableStorPoolList, thinFreeCapacitiesRef);
        long capacity = getCapacity(placeCount, availableStorPoolList);

        List<StorPoolApi> selectedStorPools = new ArrayList<>();
        ReadOnlyProps ctrpProps = getCtrlPropsPrivileged();
        if (selectedStorPoolSet != null)
        {
            for (StorPool sp : selectedStorPoolSet)
            {
                selectedStorPools.add(
                    sp.getApiData(
                        null,
                        null,
                        peerCtxProvider.get(),
                        null,
                        null,
                        FreeCapacityAutoPoolSelectorUtils.getFreeCapacityOversubscriptionRatioPrivileged(
                            sysAccCtx,
                            sp,
                            ctrpProps
                        ),
                        FreeCapacityAutoPoolSelectorUtils.getTotalCapacityOversubscriptionRatioPrivileged(
                            sysAccCtx,
                            sp,
                            ctrpProps
                        )
                    )
                );
            }
        }
        return new QuerySizeInfoResponsePojo(
            maxVlmSize,
            available,
            capacity,
            selectedStorPools
        );
    }

    private long getMaxVlmSize(
        @Nullable Set<StorPool> selectedStorPoolSetRef,
        @Nullable Map<Key, Long> thinFreeCapacitiesRef
    )
    {
        Long maxVlmSize = null;
        if (selectedStorPoolSetRef != null)
        {
            final ReadOnlyProps ctrlProps = getCtrlPropsPrivileged();
            for (StorPool sp : selectedStorPoolSetRef)
            {
                Optional<Long> optFreeCap = FreeCapacityAutoPoolSelectorUtils
                    .getFreeCapacityCurrentEstimationPrivileged(
                        sysAccCtx,
                        thinFreeCapacitiesRef,
                        sp,
                        ctrlProps,
                        true
                    );
                long freeCap = optFreeCap.orElse(0L);
                if (maxVlmSize == null || freeCap < maxVlmSize)
                {
                    maxVlmSize = freeCap;
                }
            }
        }
        return maxVlmSize == null ? 0 : maxVlmSize;
    }

    private ReadOnlyProps getCtrlPropsPrivileged()
    {
        try
        {
            return sysCfgRepo.getCtrlConfForView(sysAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private long getAvailable(
        int placeCountRef,
        @Nullable Set<StorPool> selectedStorPoolSetRef,
        List<StorPool> availableStorPoolListRef,
        @Nullable Map<Key, Long> thinFreeCapacitiesRef
    )
    {
        long available;
        if (selectedStorPoolSetRef == null)
        {
            available = 0;
        }
        else
        {
            ArrayList<Long> availableSizes = new ArrayList<>();

            final ReadOnlyProps ctrlProps = getCtrlPropsPrivileged();
            for (StorPool sp : availableStorPoolListRef)
            {
                Optional<Long> optFreeCap = FreeCapacityAutoPoolSelectorUtils
                    .getFreeCapacityCurrentEstimationPrivileged(
                        sysAccCtx,
                        thinFreeCapacitiesRef,
                        sp,
                        ctrlProps,
                        false
                    );
                if (optFreeCap.isPresent())
                {
                    availableSizes.add(optFreeCap.get());
                }
            }

            available = simulate(availableSizes, placeCountRef);
        }
        return available;
    }

    private long simulate(ArrayList<Long> sizes, int placeCount)
    {
        long ret = 0;
        while (sizes.size() >= placeCount)
        {
            sizes.sort(Long::compareTo);
            int nthHighestIdx = sizes.size() - placeCount;
            long nthHighest = sizes.get(nthHighestIdx);

            ret += nthHighest;

            for (int idx = nthHighestIdx; idx < sizes.size(); idx++)
            {
                sizes.set(idx, sizes.get(idx) - nthHighest);
            }
            int incr;
            for (int idx = 0; idx < sizes.size(); idx += incr)
            {
                if (sizes.get(idx) == 0)
                {
                    sizes.remove(idx);
                    incr = 0;
                }
                else
                {
                    incr = 1;
                }
            }
        }
        return ret;
    }

    private long getCapacity(int placeCountRef, List<StorPool> availableStorPoolListRef) throws AccessDeniedException
    {
        AccessContext peerCtx = peerCtxProvider.get();
        ArrayList<Long> capacitySizes = new ArrayList<>();
        for (StorPool sp : availableStorPoolListRef)
        {
            Optional<Long> optTotalCapacity = sp.getFreeSpaceTracker().getTotalCapacity(peerCtx);
            if (optTotalCapacity.isPresent())
            {
                capacitySizes.add(optTotalCapacity.get());
            }
        }
        return simulate(capacitySizes, placeCountRef);
    }

    public @Nullable PairNonNull<QueryAllSizeInfoResponsePojo, Double> getQasiResponse(
        QueryAllSizeInfoRequestPojo queryAllSizeInfoReqRef
    )
    {
        PairNonNull<QueryAllSizeInfoResponsePojo, Double> ret;
        synchronized (cachedQasiMap)
        {
            ret = getCached(
                cachedQasiMap,
                queryAllSizeInfoReqRef.getIgnoreCacheOlderThanSec(),
                queryAllSizeInfoReqRef.getAutoSelectFilterData()
            );
        }
        return ret;
    }

    public void cache(QueryAllSizeInfoRequestPojo queryAllSizeInfoReqRef, QueryAllSizeInfoResponsePojo responseRef)
    {
        synchronized (cachedQasiMap)
        {
            // we need to create a copy of the pojo to make sure that the values do not change, since changing the value
            // would also cause a changed hashcode, which would mess up the hashmap
            AutoSelectFilterPojo copy = AutoSelectFilterPojo.merge(queryAllSizeInfoReqRef.getAutoSelectFilterData());
            cachedQasiMap.put(copy, new CacheEntry<>(responseRef));
        }
    }

    public @Nullable PairNonNull<ApiCallRcWith<QuerySizeInfoResponsePojo>, Double> getQsiResponse(
        QuerySizeInfoRequestPojo querySizeInfoReqRef
    )
    {
        PairNonNull<ApiCallRcWith<QuerySizeInfoResponsePojo>, Double> ret;
        synchronized (cachedQsiPojoMap)
        {
            ret = getCached(
                cachedQsiPojoMap.get(querySizeInfoReqRef.getRscGrpName()),
                querySizeInfoReqRef.getIgnoreCacheOlderThanSec(),
                querySizeInfoReqRef.getAutoSelectFilterData()
            );
        }
        return ret;
    }

    public void cache(String rscGrpName,
        @Nullable AutoSelectFilterPojo autoSelectFilterPojoRef,
        ApiCallRcWith<QuerySizeInfoResponsePojo> responseRef
    )
    {
        synchronized (cachedQsiPojoMap)
        {
            Map<AutoSelectFilterPojo, CacheEntry<ApiCallRcWith<QuerySizeInfoResponsePojo>>> map = cachedQsiPojoMap
                .computeIfAbsent(rscGrpName, ignored -> new HashMap<>());

            // we need to create a copy of the pojo to make sure that the values do not change, since changing the value
            // would also cause a changed hashcode, which would mess up the hashmap
            AutoSelectFilterPojo copy = AutoSelectFilterPojo.merge(autoSelectFilterPojoRef);
            map.put(copy, new CacheEntry<>(responseRef));
        }
    }

    private <T> @Nullable PairNonNull<T, Double> getCached(
        Map<AutoSelectFilterPojo, CacheEntry<T>> cachedMapRef,
        int ignoreCacheOlderThanSecRef,
        @Nullable AutoSelectFilterApi autoSelectFilterRef
    )
    {
        PairNonNull<T, Double> ret = null;
        if (cachedMapRef != null)
        {
            CacheEntry<T> cacheEntry = cachedMapRef.get(autoSelectFilterRef);
            if (cacheEntry != null)
            {
                long now = System.currentTimeMillis();
                if (cacheEntry.cacheTimestampInMs + ignoreCacheOlderThanSecRef * SEC_TO_MS > now)
                {
                    ret = new PairNonNull<>(cacheEntry.obj, (now - cacheEntry.cacheTimestampInMs) * 1.0 / SEC_TO_MS);
                }
            }
        }
        return ret;
    }

    public void clearCache()
    {
        boolean cacheCleared;
        synchronized (cachedQasiMap)
        {
            cacheCleared = !cachedQasiMap.isEmpty();
            cachedQasiMap.clear();
        }
        synchronized (cachedQsiPojoMap)
        {
            cacheCleared |= !cachedQsiPojoMap.isEmpty();
            cachedQsiPojoMap.clear();
        }

        if (cacheCleared)
        {
            errorReporter.logDebug("QSI cache cleared");
        }
    }

    private static class CacheEntry<T>
    {
        T obj;
        long cacheTimestampInMs;

        CacheEntry(T objRef)
        {
            this(objRef, System.currentTimeMillis());
        }

        CacheEntry(T objRef, long cacheTimestampInMsRef)
        {
            obj = objRef;
            cacheTimestampInMs = cacheTimestampInMsRef;
        }
    }
}
