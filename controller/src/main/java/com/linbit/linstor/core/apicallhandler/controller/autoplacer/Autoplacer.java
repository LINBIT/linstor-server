package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.Key;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Singleton
public class Autoplacer
{
    private final StorPoolFilter filter;
    private final StrategyHandler strategyHandler;
    private final PreSelector preSelector;
    private final Selector selector;

    @Inject
    public Autoplacer(
        StorPoolFilter filterRef,
        StrategyHandler strategyHandlerRef,
        PreSelector preSelectorRef,
        Selector selectorRef
    )
    {
        filter = filterRef;
        strategyHandler = strategyHandlerRef;
        preSelector = preSelectorRef;
        selector = selectorRef;
    }

    public Optional<Set<StorPool>> autoPlace(
        AutoSelectFilterApi selectFilter,
        long rscSize,
        Map<Key, Long> freeCapacitiesRef,
        boolean includeThinRef
    )
    {
        Set<StorPool> selection;
        try
        {
            List<StorPool> availableStorPools = filter.listAvailableStorPools();

            // 1: filter storage pools
            List<StorPool> filteredStorPools = filter.filter(selectFilter, availableStorPools, rscSize);

            // 2: rate each storage pool with different weighted strategies
            Map<String, Double> weights = getWeights(selectFilter);
            Map<StorPool, Double> ratings = strategyHandler.rate(filteredStorPools, weights);

            // 3: allow the user to re-sort / filter storage pools as they see fit
            Map<StorPool, Double> preselection = preSelector.preselect(null, ratings);

            // 4: actual selection of storage pools
            selection = selector.select(selectFilter, preselection);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return Optional.ofNullable(selection);
    }

    private Map<String, Double> getWeights(AutoSelectFilterApi selectFilterRef)
    {
        // TODO: implement
        return Collections.emptyMap();
    }
}

