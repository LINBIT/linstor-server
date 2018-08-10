package com.linbit.linstor.api;

public class SpaceInfo {
    public Long totalCapacity;
    public Long freeSpace;

    public SpaceInfo(Long totalCapacityRef, Long freeSpaceRef)
    {
        totalCapacity = totalCapacityRef;
        freeSpace = freeSpaceRef;
    }
}
