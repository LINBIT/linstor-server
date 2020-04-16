package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.Key;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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
        Set<StorPool> selection = null;
        try
        {
            ArrayList<StorPool> availableStorPools = filter.listAvailableStorPools();

            // 1: filter storage pools
            ArrayList<StorPool> filteredStorPools = filter.filter(selectFilter, availableStorPools, rscSize);

            // 2: rate each storage pool with different weighted strategies
            Map<String, Double> weights = getWeights(selectFilter);
            Collection<StorPoolWithScore> storPoolsWithScoreList = strategyHandler.rate(filteredStorPools, weights);

            // 3: allow the user to re-sort / filter storage pools as they see fit
            Collection<StorPoolWithScore> preselection = preSelector.preselect(null, storPoolsWithScoreList);

            // 4: actual selection of storage pools
            Set<StorPoolWithScore> selectionWithScores = selector.select(selectFilter, preselection);
            if (selectionWithScores != null)
            {
                selection = new TreeSet<>();
                for (StorPoolWithScore spWithScore : selectionWithScores)
                {
                    selection.add(spWithScore.storPool);
                }
            }
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

    static class StorPoolWithScore implements Comparable<StorPoolWithScore>
    {
        StorPool storPool;
        double score;

        public StorPoolWithScore(StorPool storPoolRef, double scoreRef)
        {
            super();
            storPool = storPoolRef;
            score = scoreRef;
        }

        @Override
        public int compareTo(StorPoolWithScore sp2)
        {
            // highest to lowest
            return Double.compare(sp2.score, score);
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            long temp;
            temp = Double.doubleToLongBits(score);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            result = prime * result + ((storPool == null) ? 0 : storPool.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StorPoolWithScore other = (StorPoolWithScore) obj;
            if (Double.doubleToLongBits(score) != Double.doubleToLongBits(other.score))
                return false;
            if (storPool == null)
            {
                if (other.storPool != null)
                    return false;
            }
            else
                if (!storPool.equals(other.storPool))
                    return false;
            return true;
        }

        @Override
        public String toString()
        {
            return "StorPoolWithScore [storPool=" + storPool + ", score=" + score + "]";
        }
    }
}

