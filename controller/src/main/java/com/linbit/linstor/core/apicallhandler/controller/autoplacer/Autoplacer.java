package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Singleton
public class Autoplacer
{
    private final AccessContext apiAccCtx;
    private final StorPoolFilter filter;
    private final StrategyHandler strategyHandler;
    private final Selector selector;
    private final ErrorReporter errorReporter;

    @Inject
    public Autoplacer(
        @SystemContext AccessContext apiAccCtxRef,
        StorPoolFilter filterRef,
        StrategyHandler strategyHandlerRef,
        Selector selectorRef,
        ErrorReporter errorReporterRef
    )
    {
        apiAccCtx = apiAccCtxRef;
        filter = filterRef;
        strategyHandler = strategyHandlerRef;
        selector = selectorRef;
        errorReporter = errorReporterRef;
    }

    /**
     * @param selectFilter
     * @param rscDfnRef
     * @param rscSize
     * @return Null if no selection could be made of a non-empty Set of selected StorPools
     */
    public @Nullable
    Set<StorPool> autoPlace(
        AutoSelectFilterApi selectFilter,
        @Nullable ResourceDefinition rscDfnRef,
        long rscSize
    )
    {
        Set<StorPool> selection = null;
        try
        {
            Resource.Flags disklessType = Resource.Flags.valueOfOrNull(selectFilter.getDisklessType());

            long start = System.currentTimeMillis();
            ArrayList<StorPool> availableStorPools = filter.listAvailableStorPools(disklessType == null);

            // 1: filter storage pools
            long startFilter = System.currentTimeMillis();
            ArrayList<StorPool> filteredStorPools = filter.filter(
                selectFilter,
                availableStorPools,
                rscDfnRef,
                rscSize,
                disklessType
            );
            errorReporter.logTrace(
                "Autoplacer.Filter: Finished in %dms. %s StorPools remaining",
                System.currentTimeMillis() - startFilter,
                filteredStorPools.size()
            );

            // 2: rate each storage pool with different weighted strategies
            long startRating = System.currentTimeMillis();
            Collection<StorPoolWithScore> storPoolsWithScoreList = strategyHandler.rate(filteredStorPools);
            errorReporter.logTrace(
                "Autoplacer.Strategy: Finished in %dms.",
                System.currentTimeMillis() - startRating
            );

            // 3: actual selection of storage pools
            long startSelection = System.currentTimeMillis();
            Set<StorPoolWithScore> selectionWithScores = selector.select(
                selectFilter,
                rscDfnRef,
                storPoolsWithScoreList
            );
            errorReporter.logTrace(
                "Autoplacer.Selection: Finished in %dms.",
                System.currentTimeMillis() - startSelection
            );

            boolean foundCandidate = selectionWithScores != null;
            if (foundCandidate)
            {
                selection = new TreeSet<>();
                for (StorPoolWithScore spWithScore : selectionWithScores)
                {
                    selection.add(spWithScore.storPool);
                }
            }
            errorReporter.logTrace(
                "Autoplacer: Finished in %dms %s candidate",
                System.currentTimeMillis() - start,
                foundCandidate ? "with" : "without"
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return selection;
    }

    /**
     * Returns a resource (except the fixedResources) which can be deleted.
     * <p>
     * While under all circumstances all fixedResources are ignored, the selection prioritizes
     * resources that are already violating the resource-group's autoplace-config.
     * If there are no violations, the selection will choose the resource with the lowest
     * score for the data-storagePool
     *
     * @param rscDfnRef
     * @param fixedResources
     * @return
     */
    public @Nullable Resource autoUnplace(ResourceDefinition rscDfnRef, Collection<Resource> fixedResources)
    {
        @Nullable Resource ret = null;
        try
        {
            List<Node> fixedNodes = new ArrayList<>();
            List<Resource> candidatesToRemove = new ArrayList<>();
            Iterator<Resource> rscIt = rscDfnRef.iterateResource(apiAccCtx);
            Set<StorPool> storPoolList = new HashSet<>();
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                if (fixedResources.contains(rsc))
                {
                    fixedNodes.add(rsc.getNode());
                }
                else
                {
                    candidatesToRemove.add(rsc);
                    storPoolList.addAll(LayerVlmUtils.getStorPools(rsc, apiAccCtx, false));
                }
            }

            Collection<StorPoolWithScore> sortedStorPoolByScore = strategyHandler.rate(storPoolList);

            @Nullable Node unselectedNode = selector.unselect(
                rscDfnRef,
                fixedNodes,
                sortedStorPoolByScore.toArray(new StorPoolWithScore[0])
            );
            if (unselectedNode != null)
            {
                ret = rscDfnRef.getResource(apiAccCtx, unselectedNode.getName());
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    static class StorPoolWithScore implements Comparable<StorPoolWithScore>
    {
        StorPool storPool;
        double score;

        StorPoolWithScore(StorPool storPoolRef, double scoreRef)
        {
            super();
            storPool = storPoolRef;
            score = scoreRef;
        }

        @Override
        public int compareTo(StorPoolWithScore sp2)
        {
            // highest to lowest
            int cmp = Double.compare(sp2.score, score);
            if (cmp == 0)
            {
                cmp = storPool.compareTo(sp2.storPool); // by name (nodename first)
            }
            return cmp;
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
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            StorPoolWithScore other = (StorPoolWithScore) obj;
            if (Double.doubleToLongBits(score) != Double.doubleToLongBits(other.score))
            {
                return false;
            }
            if (storPool == null)
            {
                if (other.storPool != null)
                {
                    return false;
                }
            }
            else if (!storPool.equals(other.storPool))
            {
                return false;
            }
            return true;
        }

        @Override
        public String toString()
        {
            return "StorPoolWithScore [storPool=" + storPool + ", score=" + score + "]";
        }
    }
}

