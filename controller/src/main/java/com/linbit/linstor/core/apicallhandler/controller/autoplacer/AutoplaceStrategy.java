package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.List;
import java.util.Map;

public interface AutoplaceStrategy
{
    Map<StorPool, Double> rate(List<StorPool> storPools, RatingAdditionalInfo additionalInfoRef)
        throws AccessDeniedException;

    String getName();

    public static class RatingAdditionalInfo
    {
        public RatingAdditionalInfo()
        {
        }
    }
}
