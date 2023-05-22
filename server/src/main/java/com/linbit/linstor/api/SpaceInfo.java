package com.linbit.linstor.api;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.storage.StorageException;

public class SpaceInfo
{
    public Long totalCapacity;
    public Long freeCapacity;

    public SpaceInfo(Long totalCapacityRef, Long freeSpaceRef)
    {
        totalCapacity = totalCapacityRef;
        freeCapacity = freeSpaceRef;
    }

    public static SpaceInfo buildOrThrowOnError(Long totalCapacityRef, Long freeSpaceRef, StorPool spRef)
        throws StorageException
    {
        if (totalCapacityRef == null || freeSpaceRef == null)
        {
            throw new StorageException(
                "Failed to query total capacity and/or free space for storage pool: " + spRef.getName()
            );
        }
        return new SpaceInfo(totalCapacityRef, freeSpaceRef);
    }
}
