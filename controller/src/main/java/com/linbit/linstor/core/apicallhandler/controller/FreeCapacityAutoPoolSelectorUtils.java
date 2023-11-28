package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import org.jetbrains.annotations.NotNull;

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
        @Nonnull AccessContext sysCtxRef,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities,
        @Nonnull StorPool storPool,
        boolean includeOversubscriptionRatioRef
    )
    {
        long reservedCapacity = getReservedCapacityPrivileged(sysCtxRef, storPool);

        Long freeCapacityOverride = thinFreeCapacities == null ?
            null :
            thinFreeCapacities.get(new StorPool.Key(storPool));

        Optional<Long> freeCapacity = freeCapacityOverride != null ?
            Optional.of(freeCapacityOverride) :
            getFreeSpaceLastUpdatedPrivileged(sysCtxRef, storPool);

        Optional<Long> usableCapacity = freeCapacity.map(
            capacity ->
            {
                final long ret;
                boolean thinPool = storPool.getDeviceProviderKind().usesThinProvisioning();
                if (thinPool && capacity != Long.MAX_VALUE && includeOversubscriptionRatioRef)
                {
                    long oversubscriptionCapacity = (long) (capacity * getOversubscriptionRatioPrivileged(
                        sysCtxRef,
                        storPool
                    ));
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

    private static StorPool getStorPoolPrivileged(
        @Nonnull AccessContext sysCtxRef,
        @Nonnull Node nodeRef,
        @Nonnull StorPoolName storPoolNameRef
    )
    {
        StorPool storPool;
        try
        {
            storPool = nodeRef.getStorPool(sysCtxRef, storPoolNameRef);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return storPool;
    }

    private static long getReservedCapacityPrivileged(@NotNull AccessContext sysCtxRef, @Nonnull StorPool storPoolRef)
    {
        long reservedCapacity;
        try
        {
            reservedCapacity = storPoolRef.getFreeSpaceTracker().getReservedCapacity(sysCtxRef);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return reservedCapacity;
    }

    private static Optional<Long> getFreeSpaceLastUpdatedPrivileged(
        @Nonnull AccessContext sysCtxRef,
        @Nonnull StorPool storPoolRef
    )
    {
        Optional<Long> freeSpaceLastUpdated;
        try
        {
            freeSpaceLastUpdated = storPoolRef.getFreeSpaceTracker().getFreeCapacityLastUpdated(sysCtxRef);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return freeSpaceLastUpdated;
    }

    private static double getOversubscriptionRatioPrivileged(
        @Nonnull AccessContext sysCtxRef,
        @Nonnull StorPool storPoolRef
    )
    {
        double osRatio;
        try
        {
            osRatio = storPoolRef.getOversubscriptionRatio(sysCtxRef);
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
