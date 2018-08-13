package com.linbit.linstor.event.common;

public class UsageState
{
    private final Boolean resourceReady;
    private final Boolean inUse;

    public UsageState(Boolean resourceReadyRef, Boolean inUseRef)
    {
        resourceReady = resourceReadyRef;
        inUse = inUseRef;
    }

    public Boolean getResourceReady()
    {
        return resourceReady;
    }

    public Boolean getInUse()
    {
        return inUse;
    }
}
