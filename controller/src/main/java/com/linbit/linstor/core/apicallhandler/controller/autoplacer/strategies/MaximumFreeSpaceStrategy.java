package com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoplaceStrategy;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class MaximumFreeSpaceStrategy implements AutoplaceStrategy
{
    private static final double MAX_FREE_SPACE_DFLT_WEIGHT = 1.0;
    private final AccessContext apiCtx;

    @Inject
    public MaximumFreeSpaceStrategy(
        @SystemContext AccessContext apiCtxRef
    )
    {
        apiCtx = apiCtxRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.KEY_AUTOPLACE_STRAT_WEIGHT_MAX_FREESPACE;
    }

    @Override
    public MinMax getMinMax()
    {
        return MinMax.MAXIMIZE;
    }

    @Override
    public double getDefaultWeight()
    {
        return MAX_FREE_SPACE_DFLT_WEIGHT;
    }

    @Override
    public Map<StorPool, Double> rate(Collection<StorPool> storPoolsRef, RatingAdditionalInfo additionalInfoRef)
        throws AccessDeniedException
    {
        Map<StorPool, Double> ret = new HashMap<>();
        for (StorPool sp : storPoolsRef)
        {
            long freeCapacity = sp.getFreeSpaceTracker().getFreeCapacityLastUpdated(apiCtx).orElse(0L);
            ret.put(sp, (double) freeCapacity);
        }
        return ret;
    }
}
