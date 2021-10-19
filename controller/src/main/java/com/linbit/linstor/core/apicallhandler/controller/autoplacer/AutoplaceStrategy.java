package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.Collection;
import java.util.Map;

public interface AutoplaceStrategy
{
    Map<StorPool, Double> rate(Collection<StorPool> storPools, RatingAdditionalInfo additionalInfoRef)
        throws AccessDeniedException;

    String getName();

    MinMax getMinMax();

    enum MinMax
    {
        MINIMIZE, MAXIMIZE;
    }

    class RatingAdditionalInfo
    {
        public RatingAdditionalInfo()
        {
        }
    }
}
