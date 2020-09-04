package com.linbit.linstor.event.common;

import javax.annotation.Nullable;

public class ResourceState
{
    private final Boolean resourceReady;
    private final Boolean inUse;
    private final Boolean upToDate;
    private final Integer promotionScore;
    private final Boolean mayPromote;

    public ResourceState(
        Boolean resourceReadyRef,
        Boolean inUseRef,
        Boolean upToDateRef,
        @Nullable Integer promotionScoreRef,
        @Nullable Boolean mayPromoteRef)
    {
        resourceReady = resourceReadyRef;
        inUse = inUseRef;
        upToDate = upToDateRef;
        promotionScore = promotionScoreRef;
        mayPromote = mayPromoteRef;
    }

    public Boolean getResourceReady()
    {
        return resourceReady;
    }

    public Boolean getInUse()
    {
        return inUse;
    }

    public Boolean getUpToDate()
    {
        return upToDate;
    }

    public @Nullable Integer getPromotionScore()
    {
        return promotionScore;
    }

    public @Nullable Boolean mayPromote()
    {
        return mayPromote;
    }
}
