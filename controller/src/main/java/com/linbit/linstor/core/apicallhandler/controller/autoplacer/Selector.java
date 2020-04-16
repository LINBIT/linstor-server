package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Singleton
class Selector
{
    private final AccessContext apiCtx;

    @Inject
    Selector(@SystemContext AccessContext apiCtxRef)
    {
        apiCtx = apiCtxRef;

    }

    public Set<StorPoolWithScore> select(
        AutoSelectFilterApi selectFilterRef,
        Collection<StorPoolWithScore> storPoolWithScores
    )
        throws AccessDeniedException
    {
        StorPoolWithScore[] sortedStorPoolByScoreArr = storPoolWithScores.toArray(new StorPoolWithScore[0]);
        Arrays.sort(sortedStorPoolByScoreArr);

        Set<StorPoolWithScore> selectionResult = null;

        Set<StorPoolWithScore> currentSelection;
        int startIdx = 0;
        double selectionScore = Double.MIN_VALUE;
        final Integer replicaCount = selectFilterRef.getReplicaCount();
        boolean keepSearchingForCandidates = true;
        do
        {
            currentSelection = findSelection(
                startIdx,
                sortedStorPoolByScoreArr,
                new SelectionManger(selectFilterRef)
            );
            if (currentSelection.size() == replicaCount)
            {
                double currentScore = 0;
                for (StorPoolWithScore spWithScore : currentSelection)
                {
                    currentScore += spWithScore.score;
                }

                if (currentScore > selectionScore)
                {
                    selectionResult = currentSelection;
                    selectionScore = currentScore;
                }
                startIdx++;
                if (startIdx <= sortedStorPoolByScoreArr.length - replicaCount)
                {
                    double nextHighestPossibleScore = 0;
                    for (int idx = 0; idx < replicaCount; idx++)
                    {
                        /*
                         * we ignore here all filters and node-assignments, etc... we just want to
                         * verify if we should be keep searching for better candidates or not
                         */
                        nextHighestPossibleScore += sortedStorPoolByScoreArr[idx + startIdx].score;
                    }
                    keepSearchingForCandidates = nextHighestPossibleScore > selectionScore;
                }
                else
                {
                    keepSearchingForCandidates = false;
                }
            }
        } while (currentSelection.size() == replicaCount && keepSearchingForCandidates);

        return selectionResult;
    }

    private Set<StorPoolWithScore> findSelection(
        int startIdxRef,
        StorPoolWithScore[] sortedStorPoolByScoreArrRef,
        SelectionManger currentSelection
    )
        throws AccessDeniedException
    {
        for (int idx = startIdxRef; idx < sortedStorPoolByScoreArrRef.length && !currentSelection.isComplete(); idx++)
        {
            StorPoolWithScore currentSpWithScore = sortedStorPoolByScoreArrRef[idx];
            if (currentSelection.chooseIfAllowed(currentSpWithScore))
            {
                Set<StorPoolWithScore> childStorPoolSelection = findSelection(
                    idx + 1,
                    sortedStorPoolByScoreArrRef,
                    currentSelection
                );
                if (childStorPoolSelection == null)
                {
                    /*
                     * recursion could not finish, i.e. the current selection does not allow enough storage pools
                     * remove our selection and retry with the next storage pool
                     */
                    currentSelection.unselect(currentSpWithScore);
                }
            }
        }
        return currentSelection.selectedStorPoolWithScoreSet;
    }

    /**
     * This class has two purposes:
     * First, it has to perform a fast verification if a given storage pool can be selected
     * (this step needs to consider rules like only one storage pool per node, replicas on same,
     * replicas on different, etc...)
     * Second, it has to be able to rollback such a
     */
    private class SelectionManger
    {
        private final AutoSelectFilterApi selectFilter;
        private final Set<Node> selectedNodes;
        private final Set<StorPoolWithScore> selectedStorPoolWithScoreSet;

        /*
         * temporary maps, extended when a storage pool is added and
         * recalculated when a storage pool is removed
         */
        private HashMap<String, String> sameProps = new HashMap<>();
        private HashMap<String, List<String>> diffProps = new HashMap<>();

        public SelectionManger(AutoSelectFilterApi selectFilterRef) throws AccessDeniedException
        {
            selectFilter = selectFilterRef;

            selectedNodes = new HashSet<>();
            selectedStorPoolWithScoreSet = new HashSet<>();

            rebuildTemporaryMaps();
        }

        public boolean isComplete()
        {
            return selectedStorPoolWithScoreSet.size() == selectFilter.getReplicaCount();
        }

        public boolean chooseIfAllowed(StorPoolWithScore currentSpWithScoreRef) throws AccessDeniedException
        {
            Node node = currentSpWithScoreRef.storPool.getNode();
            Props nodeProps = node.getProps(apiCtx);

            boolean isAllowed = !selectedNodes.contains(node);

            // checking same props
            Iterator<Entry<String, String>> samePropEntrySetIterator = sameProps.entrySet().iterator();
            while (isAllowed && samePropEntrySetIterator.hasNext())
            {
                Entry<String, String> sameProp = samePropEntrySetIterator.next();
                String samePropValue = sameProp.getValue();
                if (samePropValue != null)
                {
                    String nodePropValue = nodeProps.getProp(sameProp.getKey());
                    isAllowed = nodePropValue == null || nodePropValue.equals(samePropValue);
                }
            }
            // checking diff props
            Iterator<Entry<String, List<String>>> diffPropEntrySetIterator = diffProps.entrySet().iterator();
            while (isAllowed && diffPropEntrySetIterator.hasNext())
            {
                Entry<String, List<String>> diffProp = diffPropEntrySetIterator.next();

                String nodePropValue = nodeProps.getProp(diffProp.getKey());
                if (nodePropValue != null)
                {
                    List<String> diffPropValue = diffProp.getValue();
                    isAllowed = !diffPropValue.contains(nodePropValue);
                }
            }

            if (isAllowed)
            {
                select(currentSpWithScoreRef);
            }

            return isAllowed;
        }

        private void select(StorPoolWithScore currentSpWithScoreRef) throws AccessDeniedException
        {
            StorPool currentStorPool = currentSpWithScoreRef.storPool;
            Props nodeProps = currentStorPool.getNode().getProps(apiCtx);

            // update same props
            Map<String, String> updateEntriesForSameProps = new HashMap<>(); // prevent concurrentModificationException
            for (Entry<String, String> sameProp : sameProps.entrySet())
            {
                if (sameProp.getValue() == null)
                {
                    String key = sameProp.getKey();
                    String propValue = nodeProps.getProp(key);
                    if (propValue != null)
                    {
                        updateEntriesForSameProps.put(key, propValue);
                    }
                }
            }
            sameProps.putAll(updateEntriesForSameProps);

            // update diff props
            for (Entry<String, List<String>> diffProp : diffProps.entrySet())
            {
                String key = diffProp.getKey();
                String propValue = nodeProps.getProp(key);
                if (propValue != null)
                {
                    diffProp.getValue().add(propValue);
                }
            }

            selectedStorPoolWithScoreSet.add(currentSpWithScoreRef);
            selectedNodes.add(currentStorPool.getNode());
        }

        private void unselect(StorPoolWithScore currentSpWithScoreRef) throws AccessDeniedException
        {
            selectedStorPoolWithScoreSet.remove(currentSpWithScoreRef);
            selectedNodes.remove(currentSpWithScoreRef.storPool.getNode());

            rebuildTemporaryMaps();
        }

        /*
         * This method could be implemented much more performant. However this would need
         * a bit more clever strategy for rolling back those maps
         */
        private void rebuildTemporaryMaps() throws AccessDeniedException
        {
            sameProps.clear();
            for (String replOnSame : selectFilter.getReplicasOnSameList())
            {
                String key;
                String selectedValue;

                /*
                 * Keys with values fixed by the user are already considered in the Filter step.
                 * That means we can rely here that all given storage pools already meet the
                 * fixed-value filters.
                 */
                if (!replOnSame.contains("="))
                {
                    key = replOnSame;
                    selectedValue = null;
                    for (Node selectedNode : selectedNodes)
                    {
                        String selectedNodeValue = selectedNode.getProps(apiCtx).getProp(key);
                        if (selectedNodeValue != null)
                        {
                            selectedValue = selectedNodeValue;
                            /*
                             * all other nodes of the selectedNodes set have to have the same value
                             * otherwise they should not be in the list.
                             */
                            break;
                        }
                    }
                    sameProps.put(key, selectedValue);
                }
            }

            diffProps.clear();
            for (String replOnDiff : selectFilter.getReplicasOnDifferentList())
            {
                String key;
                int assignIdx = replOnDiff.indexOf("=");
                List<String> list = new ArrayList<>();

                if (assignIdx == -1)
                {
                    key = replOnDiff;
                }
                else
                {
                    key = replOnDiff.substring(0, assignIdx);
                    list.add(replOnDiff.substring(assignIdx + 1));
                }
                for (Node selectedNode : selectedNodes)
                {
                    String selectedNodeValue = selectedNode.getProps(apiCtx).getProp(key);
                    if (selectedNodeValue != null)
                    {
                        list.add(selectedNodeValue);
                    }
                }
                diffProps.put(key, list);
            }
        }
    }
}

