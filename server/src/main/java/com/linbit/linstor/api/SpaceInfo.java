package com.linbit.linstor.api;

public class SpaceInfo {
    public Long totalCapacity;
    public Long freeCapacity;

    public SpaceInfo(Long totalCapacityRef, Long freeSpaceRef)
    {
        totalCapacity = totalCapacityRef;
        freeCapacity = freeSpaceRef;
    }
}
