package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
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
    private final ErrorReporter errorReporter;

    @Inject
    Selector(@SystemContext AccessContext apiCtxRef, ErrorReporter errorReporterRef)
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;

    }

    public Set<StorPoolWithScore> select(
        AutoSelectFilterApi selectFilterRef,
        ResourceDefinition rscDfnRef,
        Collection<StorPoolWithScore> storPoolWithScores
    )
        throws AccessDeniedException
    {
        StorPoolWithScore[] sortedStorPoolByScoreArr = storPoolWithScores.toArray(new StorPoolWithScore[0]);
        Arrays.sort(sortedStorPoolByScoreArr);

        for (StorPoolWithScore storPoolWithScore : sortedStorPoolByScoreArr)
        {
            errorReporter.logTrace(
                "Autoplacer.Selector: Score: %f, Storage pool '%s' on node '%s'",
                storPoolWithScore.score,
                storPoolWithScore.storPool.getName().displayValue,
                storPoolWithScore.storPool.getNode().getName().displayValue
            );
        }

        List<Node> alreadyDeployedOnNodes = new ArrayList<>();
        if (rscDfnRef != null)
        {
            Iterator<Resource> rscIt = rscDfnRef.iterateResource(apiCtx);
            List<String> nodeStrList = new ArrayList<>();
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                Node node = rsc.getNode();
                alreadyDeployedOnNodes.add(node);
                nodeStrList.add(node.getName().displayValue);
            }
            if (alreadyDeployedOnNodes.isEmpty())
            {
                errorReporter.logTrace(
                    "Auoplacer.Selector: Resource '%s' not deployed yet.",
                    rscDfnRef.getName().displayValue
                );
            }
            else
            {
                errorReporter.logTrace(
                    "Autoplacer.Selector: Resource '%s' already deployed on nodes: %s",
                    rscDfnRef.getName().displayValue,
                    nodeStrList.toString()
                );
            }
        }

        Set<StorPoolWithScore> selectionResult = null;

        Set<StorPoolWithScore> currentSelection;
        int startIdx = 0;
        double selectionScore = Double.MIN_VALUE;
        final Integer replicaCount = selectFilterRef.getReplicaCount();
        boolean keepSearchingForCandidates = true;
        SelectionManger selectionManger = new SelectionManger(selectFilterRef, alreadyDeployedOnNodes);
        do
        {
            currentSelection = findSelection(
                startIdx,
                sortedStorPoolByScoreArr,
                selectionManger
            );
            if (currentSelection.size() == replicaCount)
            {
                double currentScore = 0;

                StringBuilder storPoolDescrForLog = new StringBuilder();
                for (StorPoolWithScore spWithScore : currentSelection)
                {
                    currentScore += spWithScore.score;
                    storPoolDescrForLog.append("StorPool '")
                        .append(spWithScore.storPool.getName().displayValue)
                        .append("' on node '")
                        .append(spWithScore.storPool.getNode().getName().displayValue)
                        .append("' with score: ")
                        .append(spWithScore.score)
                        .append(", ");
                }
                storPoolDescrForLog.setLength(storPoolDescrForLog.length()-2);

                if (currentScore > selectionScore)
                {
                    selectionResult = currentSelection;
                    selectionScore = currentScore;
                    errorReporter.logTrace(
                        "Autoplacer.Selector: Found candidate %s with accumulated score %f",
                        storPoolDescrForLog.toString(),
                        selectionScore
                    );
                }
                else
                {
                    errorReporter.logTrace(
                        "Autoplacer.Selector: Skipping candidate %s as its accumulated score %f is lower than currently best candidate's %f",
                        storPoolDescrForLog.toString(),
                        currentScore,
                        selectFilterRef
                    );
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
                    if (!keepSearchingForCandidates)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Selector: Remaining candidates-combinations cannot have higher score then the currently chosen one. Search finished."
                        );
                    }
                    else
                    {
                        // continue the search, reset temporary maps
                        selectionManger.clear();
                    }
                }
                else
                {
                    keepSearchingForCandidates = false;
                    errorReporter.logTrace(
                        "Autoplacer.Selector: Not enough remaining storage pools left. Search finished."
                    );
                }

            }
            else
            {
                errorReporter.logTrace("Autoplacer.Selector: no more candidates found");
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
        private final List<Node> alreadyDeployedOnNodes;

        private final AutoSelectFilterApi selectFilter;
        private final Set<Node> selectedNodes;
        private final Set<StorPoolWithScore> selectedStorPoolWithScoreSet;

        /*
         * temporary maps, extended when a storage pool is added and
         * recalculated when a storage pool is removed
         */
        private HashMap<String, String> sameProps = new HashMap<>();
        private HashMap<String, List<String>> diffProps = new HashMap<>();

        public SelectionManger(AutoSelectFilterApi selectFilterRef, List<Node> alreadyDeployedOnNodesRef)
            throws AccessDeniedException
        {
            selectFilter = selectFilterRef;
            alreadyDeployedOnNodes = alreadyDeployedOnNodesRef;

            selectedNodes = new HashSet<>();
            selectedStorPoolWithScoreSet = new HashSet<>();

            clear();
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

            if (!isAllowed)
            {
                errorReporter.logTrace(
                    "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                    "canditate-selection as another StorPool was already selected from this node ",
                    currentSpWithScoreRef.storPool.getName().displayValue,
                    currentSpWithScoreRef.storPool.getNode().getName().displayValue
                );
            }

            // checking same props
            Iterator<Entry<String, String>> samePropEntrySetIterator = sameProps.entrySet().iterator();
            while (isAllowed && samePropEntrySetIterator.hasNext())
            {
                Entry<String, String> sameProp = samePropEntrySetIterator.next();
                String samePropValue = sameProp.getValue();
                if (samePropValue != null)
                {
                    String nodePropValue = nodeProps.getProp(sameProp.getKey());
                    // if the node does not have the property, do not allow selecting this storage pool
                    isAllowed = nodePropValue != null && nodePropValue.equals(samePropValue);
                    if (!isAllowed) {
                        errorReporter.logTrace(
                            "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                            "canditate-selection as the node has property '%s' set to '%s' while the already selected "+
                            "nodes require the value to be '%s'",
                            currentSpWithScoreRef.storPool.getName().displayValue,
                            currentSpWithScoreRef.storPool.getNode().getName().displayValue,
                            sameProp.getKey(),
                            nodePropValue,
                            samePropValue
                        );
                    }
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
                    if (!isAllowed)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                            "canditate-selection as the node has property '%s' set to '%s', but that value is already " +
                            "taken by another node from the current selection",
                            currentSpWithScoreRef.storPool.getName().displayValue,
                            currentSpWithScoreRef.storPool.getNode().getName().displayValue,
                            diffProp.getKey(),
                            nodePropValue
                        );
                    }
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

            errorReporter.logTrace(
                "Autoplacer.Selector: Adding StorPool '%s' on Node '%s' to current selection",
                currentSpWithScoreRef.storPool.getName().displayValue,
                currentSpWithScoreRef.storPool.getNode().getName().displayValue
            );

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

            errorReporter.logTrace(
                "Autoplacer.Selector: Removing StorPool '%s' on Node '%s' to current selection",
                currentSpWithScoreRef.storPool.getName().displayValue,
                currentSpWithScoreRef.storPool.getNode().getName().displayValue
            );

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

        private void clear() throws AccessDeniedException
        {
            selectedNodes.clear();
            selectedNodes.addAll(alreadyDeployedOnNodes);

            selectedStorPoolWithScoreSet.clear();
            rebuildTemporaryMaps();
        }
    }
}

