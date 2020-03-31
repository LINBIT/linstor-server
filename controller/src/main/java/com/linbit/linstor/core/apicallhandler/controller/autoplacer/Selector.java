package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Singleton
public class Selector
{
    private final AccessContext apiCtx;

    @Inject
    Selector(@SystemContext AccessContext apiCtxRef)
    {
        apiCtx = apiCtxRef;

    }

    public Set<StorPool> select(
        AutoSelectFilterApi selectFilterRef,
        Map<StorPool, Double> scoreMap
    )
        throws AccessDeniedException
    {
        StorPool[] sortedStorPoolArr = scoreMap.keySet().toArray(new StorPool[0]);

        // compare sp2 with sp1 so that we get a decreasing order (best to worst)
        Arrays.sort(sortedStorPoolArr, (sp1, sp2) -> Double.compare(scoreMap.get(sp2), scoreMap.get(sp1)));

        Set<StorPool> selectionResult = null;

        Set<StorPool> currentSelection;
        int startIdx = 0;
        double nextHighestPossibleScore = Double.MIN_VALUE;
        double selectionScore = Double.MIN_VALUE;
        final Integer replicaCount = selectFilterRef.getReplicaCount();
        do
        {
            currentSelection = findSelection(
                startIdx,
                sortedStorPoolArr,
                new SelectionManger(selectFilterRef)
            );
            if (currentSelection.size() == replicaCount)
            {
                double currentScore = 0;
                for (StorPool sp : currentSelection)
                {
                    currentScore += scoreMap.get(sp);
                }

                if (currentScore > selectionScore)
                {
                    selectionResult = currentSelection;
                    selectionScore = currentScore;
                }
                startIdx++;
                nextHighestPossibleScore = 0;
                for (int idx = 0; idx < replicaCount; idx++)
                {
                    /*
                     * we ignore here all filters and node-assignments, etc... we just want to
                     * verify if we should be keep searching for better candidates or not
                     */
                    nextHighestPossibleScore += scoreMap.get(sortedStorPoolArr[idx + startIdx]);
                }
            }
        } while (
            currentSelection.size() == replicaCount &&
            startIdx <= sortedStorPoolArr.length - replicaCount &&
            nextHighestPossibleScore > selectionScore
        );

        return selectionResult;
    }

    private Set<StorPool> findSelection(
        int startIdxRef,
        StorPool[] sortedStorPoolArrRef,
        SelectionManger currentSelection
    )
        throws AccessDeniedException
    {
        for (int idx = startIdxRef; idx < sortedStorPoolArrRef.length && !currentSelection.isComplete(); idx++)
        {
            StorPool currentSp = sortedStorPoolArrRef[idx];
            if (currentSelection.chooseIfAllowed(currentSp))
            {
                Set<StorPool> childStorPoolSelection = findSelection(
                    idx + 1,
                    sortedStorPoolArrRef,
                    currentSelection
                );
                if (childStorPoolSelection == null)
                {
                    /*
                     * recursion could not finish, i.e. the current selection does not allow enough storage pools
                     * remove our selection and retry with the next storage pool
                     */
                    currentSelection.unselect(currentSp);
                }
            }
        }
        return currentSelection.selectedStorPools;
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
        private final Set<StorPool> selectedStorPools;

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
            selectedStorPools = new HashSet<>();

            rebuildTemporaryMaps();
        }

        public boolean isComplete()
        {
            return selectedStorPools.size() == selectFilter.getReplicaCount();
        }

        public boolean chooseIfAllowed(StorPool currentSpRef) throws AccessDeniedException
        {
            Node node = currentSpRef.getNode();
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
                select(currentSpRef);
            }

            return isAllowed;
        }

        private void select(StorPool currentSpRef) throws AccessDeniedException
        {
            Props spProps = currentSpRef.getProps(apiCtx);

            // update same props
            Map<String, String> updateEntriesForSameProps = new HashMap<>(); // prevent concurrentModificationException
            for (Entry<String, String> sameProp : sameProps.entrySet())
            {
                if (sameProp.getValue() == null)
                {
                    String key = sameProp.getKey();
                    String propValue = spProps.getProp(key);
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
                String propValue = spProps.getProp(key);
                if (propValue != null)
                {
                    diffProp.getValue().add(propValue);
                }
            }

            selectedStorPools.add(currentSpRef);
            selectedNodes.add(currentSpRef.getNode());
        }

        private void unselect(StorPool currentSpRef) throws AccessDeniedException
        {
            selectedStorPools.remove(currentSpRef);
            selectedNodes.remove(currentSpRef.getNode());

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
                /*
                 * Keys with values fixed by the user are already considered in the Filter step.
                 * That means we can rely here that all given storage pools already meet the
                 * fixed-value filters.
                 */
                if (!replOnDiff.contains("="))
                {
                    key = replOnDiff;
                    List<String> list = new ArrayList<>();
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
}

