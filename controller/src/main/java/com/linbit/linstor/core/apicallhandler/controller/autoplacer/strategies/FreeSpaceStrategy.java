package com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoplaceStrategy;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class FreeSpaceStrategy implements AutoplaceStrategy
{
    private final AccessContext apiCtx;

    @Inject
    public FreeSpaceStrategy(
        @SystemContext AccessContext apiCtxRef
    )
    {
        apiCtx = apiCtxRef;
    }

    @Override
    public String getName()
    {
        return "FreeSpace";
    }

    @Override
    public Map<StorPool, Double> rate(List<StorPool> storPoolsRef) throws AccessDeniedException
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
