package com.linbit.linstor.api.pojo;

import java.util.UUID;

public class CapacityInfoPojo
{
    private final UUID storPoolUuid;
    private final String storPoolName;
    private final long freeCapacity;
    private final long totalCapacity;

    public CapacityInfoPojo(
        UUID storPoolUuidRef,
        String storPoolNameRef,
        long freeSpaceRef,
        long totalCapacityRef
    )
    {
        storPoolUuid = storPoolUuidRef;
        storPoolName = storPoolNameRef;
        freeCapacity = freeSpaceRef;
        totalCapacity = totalCapacityRef;
    }

    public UUID getStorPoolUuid()
    {
        return storPoolUuid;
    }

    public String getStorPoolName()
    {
        return storPoolName;
    }

    public long getFreeCapacity()
    {
        return freeCapacity;
    }

    public long getTotalCapacity()
    {
        return totalCapacity;
    }
}
