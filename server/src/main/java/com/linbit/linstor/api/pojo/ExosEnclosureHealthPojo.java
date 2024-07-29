package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

@Deprecated(forRemoval = true)
public class ExosEnclosureHealthPojo implements Comparable<ExosEnclosureHealthPojo>
{
    private final String enclosureName;
    private final String ctrlAIp;
    private final @Nullable String ctrlBIp;
    private final @Nullable String health;
    private final @Nullable String healthReason;

    public ExosEnclosureHealthPojo(
        final String enclosureNameRef,
        final String ctrlAIpRef,
        @Nullable final String ctrlBIpRef,
        @Nullable final String healthRef,
        @Nullable final String healthReasonRef
    )
    {
        enclosureName = enclosureNameRef;
        ctrlAIp = ctrlAIpRef;
        ctrlBIp = ctrlBIpRef;
        health = healthRef;
        healthReason = healthReasonRef;
    }

    public String getEnclosureName()
    {
        return enclosureName;
    }

    public String getCtrlAIp()
    {
        return ctrlAIp;
    }

    public @Nullable String getCtrlBIp()
    {
        return ctrlBIp;
    }

    public @Nullable String getHealth()
    {
        return health;
    }

    public @Nullable String getHealthReason()
    {
        return healthReason;
    }

    @Override
    public int compareTo(ExosEnclosureHealthPojo otherRef)
    {
        return enclosureName.compareTo(otherRef.enclosureName);
    }

}
