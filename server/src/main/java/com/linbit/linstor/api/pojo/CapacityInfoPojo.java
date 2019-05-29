package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.ApiCallRc;

import java.util.UUID;

public class CapacityInfoPojo
{
    private final UUID storPoolUuid;
    private final String storPoolName;
    private final long freeCapacity;
    private final long totalCapacity;
    private final ApiCallRc errors;

    public CapacityInfoPojo(
        UUID storPoolUuidRef,
        String storPoolNameRef,
        long freeSpaceRef,
        long totalCapacityRef,
        ApiCallRc errorsRef
    )
    {
        storPoolUuid = storPoolUuidRef;
        storPoolName = storPoolNameRef;
        freeCapacity = freeSpaceRef;
        totalCapacity = totalCapacityRef;
        errors = errorsRef;
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

    public ApiCallRc getErrors()
    {
        return errors;
    }
}
