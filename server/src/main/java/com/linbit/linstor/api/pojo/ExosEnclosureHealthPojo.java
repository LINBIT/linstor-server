package com.linbit.linstor.api.pojo;

import javax.annotation.Nullable;

@Deprecated(forRemoval = true)
public class ExosEnclosureHealthPojo implements Comparable<ExosEnclosureHealthPojo>
{
    private final String enclosureName;
    private final String ctrlAIp;
    private final String ctrlBIp;
    private final String health;
    private final String healthReason;

    public ExosEnclosureHealthPojo(
        final String enclosureNameRef,
        final String ctrlAIpRef,
        @Nullable final String ctrlBIpRef,
        final String healthRef,
        final String healthReasonRef
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

    public String getCtrlBIp()
    {
        return ctrlBIp;
    }

    public String getHealth()
    {
        return health;
    }

    public String getHealthReason()
    {
        return healthReason;
    }

    @Override
    public int compareTo(ExosEnclosureHealthPojo otherRef)
    {
        return enclosureName.compareTo(otherRef.enclosureName);
    }

}
