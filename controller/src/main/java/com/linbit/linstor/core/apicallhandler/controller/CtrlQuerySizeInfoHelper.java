package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoResponsePojo;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.StorPoolFilter;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.Key;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Singleton
public class CtrlQuerySizeInfoHelper
{
    private final Provider<AccessContext> peerCtxProvider;
    private final Autoplacer autoplacer;
    private final StorPoolFilter storPoolFilter;

    @Inject
    public CtrlQuerySizeInfoHelper(
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        Autoplacer autoplacerRef,
        StorPoolFilter storPoolFilterRef
    )
    {
        peerCtxProvider = peerCtxProviderRef;
        autoplacer = autoplacerRef;
        storPoolFilter = storPoolFilterRef;
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
        if (selectedStorPoolSet != null)
        {
            for (StorPool sp : selectedStorPoolSet)
            {
                selectedStorPools.add(sp.getApiData(null, null, peerCtxProvider.get(), null, null));
            }
        }
        return new QuerySizeInfoResponsePojo(maxVlmSize, available, capacity, selectedStorPools);
    }

    private long getMaxVlmSize(Set<StorPool> selectedStorPoolSetRef, Map<Key, Long> thinFreeCapacitiesRef)
    {
        Long maxVlmSize = null;
        if (selectedStorPoolSetRef != null)
        {
            AccessContext peerCtx = peerCtxProvider.get();
            for (StorPool sp : selectedStorPoolSetRef)
            {
                Optional<Long> optFreeCap = FreeCapacityAutoPoolSelectorUtils
                    .getFreeCapacityCurrentEstimationPrivileged(
                        peerCtx,
                        thinFreeCapacitiesRef,
                        sp
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

    private long getAvailable(
        int placeCountRef,
        Set<StorPool> selectedStorPoolSetRef,
        List<StorPool> availableStorPoolListRef,
        Map<Key, Long> thinFreeCapacitiesRef
    )
    {
        long available;
        if (selectedStorPoolSetRef == null)
        {
            available = 0;
        }
        else
        {
            AccessContext peerCtx = peerCtxProvider.get();
            ArrayList<Long> availableSizes = new ArrayList<>();
            for (StorPool sp : availableStorPoolListRef)
            {
                Optional<Long> optFreeCap = FreeCapacityAutoPoolSelectorUtils
                    .getFreeCapacityCurrentEstimationPrivileged(
                        peerCtx,
                        thinFreeCapacitiesRef,
                        sp
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
}
