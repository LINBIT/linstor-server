package com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoplaceStrategy;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Singleton
public class MinimumResourceCountStrategy implements AutoplaceStrategy
{
    private static final double MIN_RSC_COUNT_DFLT_WEIGHT = 0.00001;
    private final AccessContext apiCtx;

    @Inject
    public MinimumResourceCountStrategy(
        @SystemContext AccessContext apiCtxRef
    )
    {
        apiCtx = apiCtxRef;
    }

    @Override
    public Map<StorPool, Double> rate(Collection<StorPool> storPoolsRef, RatingAdditionalInfo additionalInfoRef)
        throws AccessDeniedException
    {
        Map<StorPool, Double> ret = new HashMap<>();

        for (StorPool sp : storPoolsRef)
        {
            /*
             * a storage pool could have multiple vlmProviderObjects of the same resource. for example
             * if the user has a pmem storage pool for cache devices, this storage pool could have
             * vlmProviderObjects from the cache layer as well as from the writecache layer
             */
            HashSet<Resource> rscSet = new HashSet<>();
            for (VlmProviderObject<Resource> vlmObj : sp.getVolumes(apiCtx))
            {
                rscSet.add(vlmObj.getRscLayerObject().getAbsResource());
            }
            ret.put(sp, (double) rscSet.size());
        }
        return ret;
    }

    @Override
    public String getName()
    {
        return ApiConsts.KEY_AUTOPLACE_STRAT_WEIGHT_MIN_RSC_COUNT;
    }

    @Override
    public MinMax getMinMax()
    {
        return MinMax.MINIMIZE;
    }

    @Override
    public double getDefaultWeight()
    {
        return MIN_RSC_COUNT_DFLT_WEIGHT;
    }
}
