package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies.FreeSpaceStrategy;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collection;
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

    public Collection<StorPoolWithScore> rate(
        List<StorPool> storPoolListRef,
        Map<String, Double> strategyWeights
    )
        throws AccessDeniedException
    {
        Map<StorPool, StorPoolWithScore> lut = new HashMap<>();
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
                StorPoolWithScore prevRating = lut.get(sp);

                if (prevRating == null)
                {
                    prevRating = new StorPoolWithScore(sp, 0);
                    lut.put(sp, prevRating);
                }
                // normalize and weight the value
                prevRating.score += rate.getValue() / highestValue * weight;
            }
        }

        return lut.values();
    }

}
