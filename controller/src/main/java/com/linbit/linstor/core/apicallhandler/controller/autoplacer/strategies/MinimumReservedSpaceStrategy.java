package com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoplaceStrategy;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class MinimumReservedSpaceStrategy implements AutoplaceStrategy
{
    private final AccessContext apiCtx;

    @Inject
    public MinimumReservedSpaceStrategy(
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
            double allocatedSum = 0;
            for (VlmProviderObject<Resource> vlmObj : sp.getVolumes(apiCtx))
            {
                long allocSize = vlmObj.getAllocatedSize();
                if (allocSize != -1)
                {
                    allocatedSum += allocSize;
                }
            }
            for (VlmProviderObject<Snapshot> snapObj : sp.getSnapVolumes(apiCtx))
            {
                long allocSize = snapObj.getAllocatedSize();
                if (allocSize != -1)
                {
                    allocatedSum += allocSize;
                }
            }

            ret.put(sp, allocatedSum);
        }

        return ret;
    }

    @Override
    public String getName()
    {
        return ApiConsts.KEY_AUTOPLACE_STRAT_WEIGHT_MIN_RESERVED_SPACE;
    }

    @Override
    public MinMax getMinMax()
    {
        return MinMax.MINIMIZE;
    }

}
