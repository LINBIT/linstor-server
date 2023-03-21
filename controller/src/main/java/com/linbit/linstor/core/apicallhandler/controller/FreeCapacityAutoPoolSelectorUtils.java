package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.Map;
import java.util.Optional;

public class FreeCapacityAutoPoolSelectorUtils
{
    /**
     * Returns whether the given storage pool is usable in terms of capacity and provisioning type.
     * If the free capacity is unknown, an empty Optional is returned.
     */
    public static Optional<Boolean> isStorPoolUsable(
        long rscSize,
        Map<StorPool.Key, Long> freeCapacities,
        boolean includeThin,
        StorPoolName storPoolName,
        Node node,
        AccessContext apiCtx
    )
    {
        StorPool storPool = getStorPoolPrivileged(apiCtx, node, storPoolName);

        Optional<Boolean> usable;
        if (storPool.getDeviceProviderKind().usesThinProvisioning() && !includeThin)
        {
            usable = Optional.of(false);
        }
        else
        {
            usable = getFreeCapacityCurrentEstimationPrivileged(
                apiCtx,
                freeCapacities,
                storPool,
                true
            ).map(freeCapacity -> freeCapacity >= rscSize);
        }
        return usable;
    }

    public static Optional<Long> getFreeCapacityCurrentEstimationPrivileged(
        AccessContext accCtx,
        Map<StorPool.Key, Long> thinFreeCapacities,
        StorPool storPool,
        boolean includeOversubscriptionRatioRef
    )
    {
        long reservedCapacity = getReservedCapacityPrivileged(accCtx, storPool);

        Long freeCapacityOverride = thinFreeCapacities == null ?
            null :
            thinFreeCapacities.get(new StorPool.Key(storPool));

        Optional<Long> freeCapacity = freeCapacityOverride != null ?
            Optional.of(freeCapacityOverride) :
            getFreeSpaceLastUpdatedPrivileged(accCtx, storPool);

        Optional<Long> usableCapacity = freeCapacity.map(
            capacity ->
            {
                final long ret;
                boolean thinPool = storPool.getDeviceProviderKind().usesThinProvisioning();
                if (thinPool && capacity != Long.MAX_VALUE && includeOversubscriptionRatioRef)
                {
                    long oversubscriptionCapacity = (long) (capacity * getOversubscriptionRatio(accCtx, storPool));
                    if (oversubscriptionCapacity < capacity)
                    {
                        // overflow
                        ret = Long.MAX_VALUE;
                    }
                    else
                    {
                        ret = oversubscriptionCapacity;
                    }
                }
                else
                {
                    ret = capacity;
                }
                return ret;
            }
        );
        return usableCapacity.map(capacity ->
        {
            final long ret;
            if (reservedCapacity < 0 && -reservedCapacity > Long.MAX_VALUE - capacity)
            {
                /*
                 * we cannot check for overflow with:
                 * c - r > M
                 * if r is negative, since (c - r) would go beyond M, missing the point of the check
                 * -r > M - c
                 * should not have this problem
                 */
                ret = Long.MAX_VALUE; // prevent overflow
            }
            else
            {
                ret = capacity - reservedCapacity;
            }
            return ret;
        });
    }

    private static StorPool getStorPoolPrivileged(AccessContext accCtx, Node node, StorPoolName storPoolName)
    {
        StorPool storPool;
        try
        {
            storPool = node.getStorPool(accCtx, storPoolName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return storPool;
    }

    private static long getReservedCapacityPrivileged(AccessContext accCtx, StorPool storPool)
    {
        long reservedCapacity;
        try
        {
            reservedCapacity = storPool.getFreeSpaceTracker().getReservedCapacity(accCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return reservedCapacity;
    }

    private static Optional<Long> getFreeSpaceLastUpdatedPrivileged(AccessContext accCtx, StorPool storPool)
    {
        Optional<Long> freeSpaceLastUpdated;
        try
        {
            freeSpaceLastUpdated = storPool.getFreeSpaceTracker().getFreeCapacityLastUpdated(accCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return freeSpaceLastUpdated;
    }

    private static double getOversubscriptionRatio(AccessContext accCtx, StorPool storPool)
    {
        double osRatio;
        try
        {
            osRatio = storPool.getOversubscriptionRatio(accCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return osRatio;
    }

    private FreeCapacityAutoPoolSelectorUtils()
    {
    }
}
