package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies.FreeSpaceStrategy;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@Singleton
class StrategyHandler
{
    private final List<AutoplaceStrategy> strategies;

    @Inject
    StrategyHandler(FreeSpaceStrategy freeSpaceStratRef)
    {
        strategies = Arrays.asList(freeSpaceStratRef);
    }

    public Map<StorPool, Double> rate(
        List<StorPool> storPoolListRef,
        Map<String, Double> strategyWeights
    )
        throws AccessDeniedException
    {
        Map<StorPool, Double> ret = new HashMap<>();
        // initialize all sub-maps
        for (StorPool sp : storPoolListRef)
        {
            ret.put(sp, 0.0);
        }

        for (AutoplaceStrategy strat : strategies)
        {
            Double weight = 1.0;
            for (Entry<String, Double> stratWeight : strategyWeights.entrySet())
            {
                if (strat.getName().equalsIgnoreCase(stratWeight.getKey()))
                {
                    weight = stratWeight.getValue();
                    break;
                }
            }
            Map<StorPool, Double> stratRate = strat.rate(storPoolListRef);
            Double highestValue = null;
            for (Double stratValues : stratRate.values())
            {
                if (highestValue == null || highestValue < stratValues)
                {
                    highestValue = stratValues;
                }
            }

            for (Entry<StorPool, Double> rate : stratRate.entrySet())
            {
                StorPool sp = rate.getKey();
                Double prevRating = ret.get(sp);

                // normalize and weight the value
                ret.put(sp, prevRating + (rate.getValue() / highestValue * weight));
            }
        }

        return ret;
    }

}
