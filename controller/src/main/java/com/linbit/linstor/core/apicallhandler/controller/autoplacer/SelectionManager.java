package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SelectionManager manages the selection of storage pools.
 *
 * In particular, it ensures that:
 * - a potential selection upholds all rules:
 *   - one storage pool per node
 *   - replicas-on-same settings
 *   - replicas-on-different settings
 * - a selection can be reverted, even in parts
 */
public class SelectionManager
{
    private final AccessContext accessContext;
    private final ErrorReporter errorReporter;
    private final List<Node> alreadyDeployedOnNodes;
    private final List<DeviceProviderKind> alreadySetProviderKindList;
    private final List<SharedStorPoolName> alreadyDeployedInSharedSPNames;

    private final AutoSelectFilterApi selectFilter;
    private final Set<Node> selectedNodes;
    private final Set<SharedStorPoolName> selectedSharedSPNames;
    private final Set<Autoplacer.StorPoolWithScore> selectedStorPoolWithScoreSet;
    private final Autoplacer.StorPoolWithScore[] sortedStorPoolByScoreArr;
    private final int additionalRscCountToSelect;
    private final ArrayList<DeviceProviderKind> selectedProviderKindList;

    /*
     * temporary maps, extended when a storage pool is added and
     * recalculated when a storage pool is removed
     */
    final private HashMap<String, String> sameProps = new HashMap<>();
    final private HashMap<String, List<String>> diffProps = new HashMap<>();

    public SelectionManager(
        AccessContext accessContextRef,
        ErrorReporter errorReporterRef,
        AutoSelectFilterApi selectFilterRef,
        List<Node> alreadyDeployedOnNodesRef,
        int diskfulNodeCount,
        int disklessNodeCount,
        List<SharedStorPoolName> alreadyDeployedInSharedSPNamesRef,
        Collection<DeviceProviderKind> alreadySelectedProviderKindsRef,
        Autoplacer.StorPoolWithScore[] sortedStorPoolByScoreArrRef
    )
        throws AccessDeniedException
    {
        accessContext = accessContextRef;
        errorReporter = errorReporterRef;
        selectFilter = selectFilterRef;
        alreadyDeployedOnNodes = alreadyDeployedOnNodesRef;
        alreadyDeployedInSharedSPNames = alreadyDeployedInSharedSPNamesRef;
        selectedProviderKindList = new ArrayList<>();
        sortedStorPoolByScoreArr = sortedStorPoolByScoreArrRef;

        if (selectFilterRef.getDisklessType() == null)
        {
            additionalRscCountToSelect = getReplicaCount(selectFilterRef, diskfulNodeCount);
            alreadySetProviderKindList = Collections.unmodifiableList(
                new ArrayList<>(alreadySelectedProviderKindsRef)
            );
        }
        else
        {
            additionalRscCountToSelect = getReplicaCount(selectFilterRef, disklessNodeCount);
            alreadySetProviderKindList = Collections.singletonList(DeviceProviderKind.DISKLESS);
        }

        selectedNodes = new HashSet<>();
        selectedSharedSPNames = new HashSet<>();
        selectedStorPoolWithScoreSet = new HashSet<>();

        clear();
    }

    public int getAdditionalRscCountToSelect()
    {
        return additionalRscCountToSelect;
    }

    private int getReplicaCount(AutoSelectFilterApi selectFilterRef, int alreadyExistingCount)
    {
        Integer ret = 0;
        Integer additional = null;
        if (
            selectFilterRef.getAdditionalReplicaCount() != null &&
                selectFilterRef.getAdditionalReplicaCount() > 0
        )
        {
            additional = selectFilterRef.getAdditionalReplicaCount();
        }

        if (selectFilterRef.getReplicaCount() != null)
        {
            if (selectFilterRef.getReplicaCount() > alreadyExistingCount)
            {
                ret = selectFilterRef.getReplicaCount() - alreadyExistingCount;
            }
            else if (additional != null)
            {
                ret = additional;
            }
        }
        else
        {
            if (additional != null)
            {
                ret = additional;
            }
        }
        return ret;
    }

    public HashSet<Autoplacer.StorPoolWithScore> findSelection(int startIdxRef) throws AccessDeniedException
    {
        clear();
        findSelectionImpl(startIdxRef);
        return new HashSet<>(selectedStorPoolWithScoreSet);
    }

    private void findSelectionImpl(int startIdxRef) throws AccessDeniedException
    {
        for (int idx = startIdxRef; idx < sortedStorPoolByScoreArr.length && !isComplete(); idx++)
        {
            Autoplacer.StorPoolWithScore currentSpWithScore = sortedStorPoolByScoreArr[idx];
            if (isAllowed(currentSpWithScore.storPool))
            {
                select(currentSpWithScore);
                findSelectionImpl(idx + 1);
                if (!isComplete())
                {
                    /*
                     * recursion could not finish, i.e. the current selection does not allow enough storage pools
                     * remove our selection and retry with the next storage pool
                     */
                    unselect(currentSpWithScore);
                }
            }
        }
    }

    private boolean isComplete()
    {
        return selectedStorPoolWithScoreSet.size() == additionalRscCountToSelect;
    }

    public boolean isAllowed(StorPool sp) throws AccessDeniedException
    {
        Node node = sp.getNode();
        Props nodeProps = node.getProps(accessContext);

        boolean isAllowed = !selectedNodes.contains(node);
        isAllowed &= !selectedSharedSPNames.contains(sp.getSharedStorPoolName());

        if (!isAllowed)
        {
            errorReporter.logTrace(
                "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                    "canditate-selection as another StorPool was already selected from this node ",
                sp.getName().displayValue,
                sp.getNode().getName().displayValue
            );
        }

        for (DeviceProviderKind selectedProviderKind : selectedProviderKindList)
        {
            if (!DeviceProviderKind.isMixingAllowed(sp.getDeviceProviderKind(), selectedProviderKind))
            {
                errorReporter.logTrace(
                    "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                        "canditate-selection as its provider kind (%s) does not match already selected (%s)",
                    sp.getName().displayValue,
                    sp.getNode().getName().displayValue,
                    sp.getDeviceProviderKind().name(),
                    selectedProviderKind.name()
                );
                isAllowed = false;
            }
        }


        // checking same props
        Iterator<Map.Entry<String, String>> samePropEntrySetIterator = sameProps.entrySet().iterator();
        while (isAllowed && samePropEntrySetIterator.hasNext())
        {
            Map.Entry<String, String> sameProp = samePropEntrySetIterator.next();
            String samePropValue = sameProp.getValue();
            if (samePropValue != null)
            {
                String nodePropValue = nodeProps.getProp(sameProp.getKey());
                // if the node does not have the property, do not allow selecting this storage pool
                isAllowed = nodePropValue != null && nodePropValue.equals(samePropValue);
                if (!isAllowed)
                {
                    errorReporter.logTrace(
                        "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                            "canditate-selection as the node has property '%s' set to '%s' while the already " +
                            "selected nodes require the value to be '%s'",
                        sp.getName().displayValue,
                        sp.getNode().getName().displayValue,
                        sameProp.getKey(),
                        nodePropValue,
                        samePropValue
                    );
                }
            }
        }
        // checking diff props
        Iterator<Map.Entry<String, List<String>>> diffPropEntrySetIterator = diffProps.entrySet().iterator();
        while (isAllowed && diffPropEntrySetIterator.hasNext())
        {
            Map.Entry<String, List<String>> diffProp = diffPropEntrySetIterator.next();

            String nodePropValue = nodeProps.getProp(diffProp.getKey());
            if (nodePropValue != null)
            {
                List<String> diffPropValue = diffProp.getValue();
                isAllowed = !diffPropValue.contains(nodePropValue);
                if (!isAllowed)
                {
                    errorReporter.logTrace(
                        "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                            "canditate-selection as the node has property '%s' set to '%s', but that value is " +
                            "already taken by another node from the current selection",
                        sp.getName().displayValue,
                        sp.getNode().getName().displayValue,
                        diffProp.getKey(),
                        nodePropValue
                    );
                }
            }
        }

        return isAllowed;
    }

    private void select(Autoplacer.StorPoolWithScore currentSpWithScoreRef) throws AccessDeniedException
    {
        StorPool currentStorPool = currentSpWithScoreRef.storPool;
        Props nodeProps = currentStorPool.getNode().getProps(accessContext);

        errorReporter.logTrace(
            "Autoplacer.Selector: Adding StorPool '%s' on Node '%s' to current selection",
            currentSpWithScoreRef.storPool.getName().displayValue,
            currentSpWithScoreRef.storPool.getNode().getName().displayValue
        );

        selectedProviderKindList.add(currentSpWithScoreRef.storPool.getDeviceProviderKind());

        // update same props
        Map<String, String> updateEntriesForSameProps = new HashMap<>(); // prevent concurrentModificationException
        for (Map.Entry<String, String> sameProp : sameProps.entrySet())
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
        for (Map.Entry<String, List<String>> diffProp : diffProps.entrySet())
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
        selectedSharedSPNames.add(currentStorPool.getSharedStorPoolName());
    }

    private void unselect(Autoplacer.StorPoolWithScore currentSpWithScoreRef) throws AccessDeniedException
    {
        StorPool sp = currentSpWithScoreRef.storPool;
        selectedStorPoolWithScoreSet.remove(currentSpWithScoreRef);
        selectedNodes.remove(sp.getNode());
        selectedSharedSPNames.remove(sp.getSharedStorPoolName());

        selectedProviderKindList.remove(currentSpWithScoreRef.storPool.getDeviceProviderKind());
        /*
         * The current selection always must contain all alreadyExistingProviderKinds
         */
        assert selectedProviderKindList.containsAll(alreadySetProviderKindList);

        errorReporter.logTrace(
            "Autoplacer.Selector: Removing StorPool '%s' on Node '%s' to current selection",
            sp.getName().displayValue,
            sp.getNode().getName().displayValue
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
        if (selectFilter.getReplicasOnSameList() != null)
        {
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
                        String selectedNodeValue = selectedNode.getProps(accessContext).getProp(key);
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
        }

        diffProps.clear();
        if (selectFilter.getReplicasOnDifferentList() != null)
        {
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
                    String selectedNodeValue = selectedNode.getProps(accessContext).getProp(key);
                    if (selectedNodeValue != null)
                    {
                        list.add(selectedNodeValue);
                    }
                }
                diffProps.put(key, list);
            }
        }
    }

    private void clear() throws AccessDeniedException
    {
        selectedNodes.clear();
        selectedNodes.addAll(alreadyDeployedOnNodes);
        selectedSharedSPNames.clear();
        selectedSharedSPNames.addAll(alreadyDeployedInSharedSPNames);

        selectedProviderKindList.clear();
        selectedProviderKindList.addAll(alreadySetProviderKindList);

        selectedStorPoolWithScoreSet.clear();
        rebuildTemporaryMaps();
    }
}
