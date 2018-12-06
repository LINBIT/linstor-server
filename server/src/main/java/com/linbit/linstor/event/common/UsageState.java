package com.linbit.linstor.event.common;

public class UsageState
{
    private final Boolean resourceReady;
    private final Boolean inUse;
    private final Boolean upToDate;

    public UsageState(Boolean resourceReadyRef, Boolean inUseRef, Boolean upToDateRef)
    {
        resourceReady = resourceReadyRef;
        inUse = inUseRef;
        upToDate = upToDateRef;
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
}
