package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.NodeSelectionStrategy;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ComparatorUtils;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

public class FreeCapacityAutoPoolSelectorUtils
{
    private static final double DEFAULT_MAX_OVERSUBSCRIPTION_RATIO = 20.;

    public static Comparator<Candidate> mostFreeCapacityCandidateStrategy(
        AccessContext accCtx,
        Map<StorPool.Key, Long> freeCapacities
    )
    {
        Comparator<StorPool> thickComparator = Comparator.<StorPool, Boolean>comparing(
            storPool -> storPool.getDriverKind().usesThinProvisioning()
        ).reversed();

        Comparator<StorPool> freeCapacityComparator = Comparator.comparingLong(
            storPool -> getFreeCapacityCurrentEstimationPrivileged(accCtx, freeCapacities, storPool).orElse(0L)
        );

        return ComparatorUtils.comparingWithComparator(
            candidate -> getCandidateStorPoolPrivileged(accCtx, candidate),
            thickComparator.thenComparing(freeCapacityComparator)
        );
    }

    public static StorPool getCandidateStorPoolPrivileged(
        AccessContext accCtx,
        Candidate candidate
    )
    {
        return getStorPoolPrivileged(
            accCtx,
            // get the stor pool for the last node, which will be the one with the least free capacity
            candidate.nodes.get(candidate.nodes.size() - 1),
            candidate.getStorPoolName()
        );
    }

    public static NodeSelectionStrategy mostFreeCapacityNodeStrategy(Map<StorPool.Key, Long> freeCapacities)
    {
        return (storPoolName, accCtx) -> Comparator.comparingLong(node ->
            getFreeCapacityCurrentEstimationPrivileged(
                accCtx,
                freeCapacities,
                getStorPoolPrivileged(accCtx, node, storPoolName)
            ).orElse(0L)
        );
    }

    public static StorPool getStorPoolPrivileged(AccessContext accCtx, Node node, StorPoolName storPoolName)
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

    public static Optional<Long> getFreeCapacityCurrentEstimationPrivileged(
        AccessContext accCtx,
        Map<StorPool.Key, Long> freeCapacities,
        StorPool storPool
    )
    {
        long reservedCapacity = getReservedCapacityPrivileged(accCtx, storPool);

        Long freeCapacityOverride = freeCapacities.get(new StorPool.Key(storPool));

        Optional<Long> freeCapacity = freeCapacityOverride != null ?
            Optional.of(freeCapacityOverride) : getFreeSpaceLastUpdatedPrivileged(accCtx, storPool);

        Optional<Long> usableCapacity = freeCapacity.map(
            capacity -> storPool.getDriverKind().usesThinProvisioning() ?
                (long) (capacity * getMaxOversubscriptionRatio(accCtx, storPool)) :
                capacity
        );

        return usableCapacity.map(capacity -> capacity - reservedCapacity);
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
            freeSpaceLastUpdated = storPool.getFreeSpaceTracker().getFreeSpaceLastUpdated(accCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return freeSpaceLastUpdated;
    }

    private static double getMaxOversubscriptionRatio(AccessContext accCtx, StorPool storPool)
    {
        String maxOversubscriptionRatioRaw = getMaxOversubscriptionRatioRaw(accCtx, storPool);
        double maxOversubscriptionRatio;
        if (maxOversubscriptionRatioRaw == null)
        {
            maxOversubscriptionRatio = DEFAULT_MAX_OVERSUBSCRIPTION_RATIO;
        }
        else
        {
            try
            {
                maxOversubscriptionRatio = Double.valueOf(maxOversubscriptionRatioRaw);
            }
            catch (NumberFormatException exc)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_PROP,
                    "Invalid value for max oversubscription ratio '" + maxOversubscriptionRatioRaw + "'"
                ));
            }
        }
        return maxOversubscriptionRatio;
    }

    private static String getMaxOversubscriptionRatioRaw(AccessContext accCtx, StorPool storPool)
    {
        String maxOversubscriptionRatioRaw;
        try
        {
            maxOversubscriptionRatioRaw = storPool.getDefinition(accCtx).getProps(accCtx)
                .getProp(ApiConsts.KEY_STOR_POOL_DFN_MAX_OVERSUBSCRIPTION_RATIO);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return maxOversubscriptionRatioRaw;
    }

    private FreeCapacityAutoPoolSelectorUtils()
    {
    }
}
