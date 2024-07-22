package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * SelectionManager manages the selection of storage pools.
 *
 * In particular, it ensures that:
 * - a potential selection upholds all rules:
 * -- one storage pool per node
 * -- replicas-on-same settings
 * -- replicas-on-different settings
 * - a selection can be reverted, even in parts
 */
public class SelectionManager
{
    private static final int X_REPLICAS_USAGE_NOT_ALLOWED = -1;
    public static final int X_REPLICAS_DFLT_COUNT = 1;

    /** DRBD Version 9.1.0 */
    private static final Version DFLT_DISKLESS_DRBD_VERSION = new Version(9, 1, 0);

    private static final String REPL_ON_SAME_UNDECIDED = null;


    private final AccessContext accessContext;
    private final ErrorReporter errorReporter;

    private final Autoplacer.StorPoolWithScore[] sortedStorPoolByScoreArr;
    private final boolean allowStorPoolMixing;

    private final LinkedList<State> selectionStack = new LinkedList<>();
    private final HashMap<String, Integer> xReplicasOnDiffCount = new HashMap<>();

    public SelectionManager(
        AccessContext accessContextRef,
        ErrorReporter errorReporterRef,
        AutoSelectFilterApi selectFilterRef,
        List<Node> alreadyDeployedOnNodesRef,
        int diskfulNodeCount,
        int disklessNodeCount,
        List<SharedStorPoolName> alreadyDeployedInSharedSPNamesRef,
        Map<DeviceProviderKind, List</* DrbdVersion */Version>> alreadyDeployedProviderKindsRef,
        Autoplacer.StorPoolWithScore[] sortedStorPoolByScoreArrRef,
        boolean allowStorPoolMixingRef
    )
        throws AccessDeniedException
    {
        accessContext = accessContextRef;
        errorReporter = errorReporterRef;
        sortedStorPoolByScoreArr = sortedStorPoolByScoreArrRef;

        boolean tmpAllowMixing = allowStorPoolMixingRef;
        for (Node alreadyDeployedNode : alreadyDeployedOnNodesRef)
        {
            Version drbdVersion = getDrbdVersion(alreadyDeployedNode);
            if (!DeviceProviderKind.doesDrbdVersionSupportStorPoolMixing(drbdVersion))
            {
                tmpAllowMixing = false;
            }
        }
        allowStorPoolMixing = tmpAllowMixing;

        final Map<DeviceProviderKind, List<Version>> initDeployedProviderKindsToDrbdVersionsRef;
        if (selectFilterRef.getDisklessType() == null)
        {
            HashMap<DeviceProviderKind, List<Version>> tmpMap = new HashMap<>();
            for (Entry<DeviceProviderKind, List<Version>> entry : alreadyDeployedProviderKindsRef.entrySet())
            {
                tmpMap.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
            }
            initDeployedProviderKindsToDrbdVersionsRef = Collections.unmodifiableMap(tmpMap);
        }
        else
        {
            initDeployedProviderKindsToDrbdVersionsRef = Collections.singletonMap(
                DeviceProviderKind.DISKLESS,
                Collections.singletonList(DFLT_DISKLESS_DRBD_VERSION) // should not matter what version we take here
            );
        }

        final HashMap<String, String> initSameProps = initializeSameProps(selectFilterRef, alreadyDeployedOnNodesRef);

        final HashMap<String, Map<String, Integer>> initDiffProps = initializeDiffProps(
            selectFilterRef,
            alreadyDeployedOnNodesRef
        );
        selectionStack.push(
            new State(
                alreadyDeployedOnNodesRef,
                alreadyDeployedInSharedSPNamesRef,
                initDeployedProviderKindsToDrbdVersionsRef,
                new HashSet<>(),
                getReplicaCount(
                    selectFilterRef,
                    selectFilterRef.getDisklessType() == null ? diskfulNodeCount : disklessNodeCount
                ),
                initSameProps,
                initDiffProps
            )
        );
    }

    private HashMap<String, String> initializeSameProps(
        AutoSelectFilterApi selectFilterRef,
        List<Node> alreadyDeployedOnNodesRef
    )
        throws AccessDeniedException
    {
        final HashMap<String, String> initSameProps = new HashMap<>();

        if (selectFilterRef.getReplicasOnSameList() != null)
        {
            for (String replOnSame : selectFilterRef.getReplicasOnSameList())
            {
                final int assignIdx = replOnSame.indexOf("=");
                final String key;
                final String val;
                if (assignIdx != -1)
                {
                    key = replOnSame.substring(0, assignIdx);
                    val = getSameValFromNodes(key, alreadyDeployedOnNodesRef, replOnSame.substring(assignIdx + 1));
                }
                else
                {
                    key = replOnSame;
                    val = getSameValFromNodes(key, alreadyDeployedOnNodesRef, REPL_ON_SAME_UNDECIDED);
                }
                initSameProps.put(key, val);
            }
        }
        return initSameProps;
    }

    private @Nullable String getSameValFromNodes(
        String keyRef,
        List<Node> alreadyDeployedOnNodesRef,
        @Nullable String dfltIfNotUsed
    )
        throws AccessDeniedException
    {
        HashMap<String, List<String>> valToNodeListMap = getValuesToNodesListMap(keyRef, alreadyDeployedOnNodesRef);

        final @Nullable String ret;
        switch (valToNodeListMap.size())
        {
            case 0:
                ret = dfltIfNotUsed;
                break;
            case 1:
                ret = valToNodeListMap.keySet().iterator().next();
                break;
            default:
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNDECIDABLE_AUTOPLACMENT,
                        "The property in --replicas-on-same '" + keyRef + "' is already set " +
                            "on already deployed nodes with different values. Autoplacer cannot decide " +
                            "which value to continue with. Linstor found the following conflicting values: " +
                            valToNodeListMap
                    )
                );
        }
        return ret;
    }

    private HashMap<String, List<String>> getValuesToNodesListMap(String keyRef, List<Node> alreadyDeployedOnNodesRef)
        throws AccessDeniedException
    {
        HashMap<String, List<String>> valToNodeList = new HashMap<>();

        for (Node node : alreadyDeployedOnNodesRef)
        {
            ReadOnlyProps prop = node.getProps(accessContext);
            @Nullable String val = prop.getProp(keyRef);
            if (val != null)
            {
                valToNodeList.computeIfAbsent(val, ignore -> new ArrayList<>())
                    .add(node.getName().displayValue);
            }
        }
        return valToNodeList;
    }

    private HashMap<String, Map<String, Integer>> initializeDiffProps(
        AutoSelectFilterApi selectFilterRef,
        List<Node> areadyDeployedNode
    )
        throws InvalidKeyException, AccessDeniedException
    {
        final HashMap<String, Map<String, Integer>> initDiffProps = new HashMap<>();
        final Map<String, Integer> xReplicasOnDifferentMap = selectFilterRef.getXReplicasOnDifferentMap() == null ?
            new HashMap<>() :
            selectFilterRef.getXReplicasOnDifferentMap();

        if (selectFilterRef.getReplicasOnDifferentList() != null)
        {
            for (String replOnDiff : selectFilterRef.getReplicasOnDifferentList())
            {
                HashMap<String, Integer> valueToRemainingCount = new HashMap<>();
                int assignIdx = replOnDiff.indexOf("=");
                final Integer maxCount;
                String key;
                if (assignIdx != -1)
                {
                    key = replOnDiff.substring(0, assignIdx);
                    String val = replOnDiff.substring(assignIdx + 1);
                    // we are not allowed to use this value at all
                    valueToRemainingCount.put(val, X_REPLICAS_USAGE_NOT_ALLOWED);
                    // maxCount = xReplicasOnDifferentMap.computeIfAbsent(key, ignored -> X_REPLICAS_DFLT_COUNT);
                }
                else
                {
                    key = replOnDiff;
                }
                maxCount = xReplicasOnDifferentMap.computeIfAbsent(key, ignored -> X_REPLICAS_DFLT_COUNT);
                for (Node node : areadyDeployedNode)
                {
                    @Nullable String nodeVal = node.getProps(accessContext).getProp(key);
                    if (nodeVal != null)
                    {
                        final int oldRemainingCount = valueToRemainingCount.getOrDefault(nodeVal, maxCount);
                        if (oldRemainingCount > 0)
                        {
                            // if oldRemainingCount is already 0, we do not want to go to -1, since that is a special
                            // case using X_REPLICAS_USAGE_NOT_ALLOWED
                            // if we are already at -1 (X_REPLICAS_USAGE_NOT_ALLOWED) we also want to stay at that value
                            valueToRemainingCount.put(nodeVal, oldRemainingCount - 1);
                        }
                    }
                }
                xReplicasOnDiffCount.put(key, maxCount);

                initDiffProps.put(key, Collections.unmodifiableMap(valueToRemainingCount));
            }
        }
        // also there is the possibility that we receive entries in the xReplicasOnDiffMap which were not present in the
        // replicasOnDiffList
        for (Entry<String, Integer> xReplEntry : xReplicasOnDifferentMap.entrySet())
        {
            String propKey = xReplEntry.getKey();
            if (!xReplicasOnDiffCount.containsKey(propKey))
            {
                xReplicasOnDiffCount.put(propKey, xReplEntry.getValue());
            }
            if (!initDiffProps.containsKey(propKey))
            {
                initDiffProps.put(propKey, Collections.emptyMap());
            }
        }
        return initDiffProps;
    }

    public int getAdditionalRscCountToSelect()
    {
        return getCurrentState().remainingRscCountToSelect;
    }

    private State getCurrentState()
    {
        final @Nullable State curState = selectionStack.peek();
        if (curState == null)
        {
            throw new ImplementationError("Current state is unexpectedly null.");
        }
        return curState;
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
        findSelectionImpl(startIdxRef);
        return new HashSet<>(getCurrentState().selectedStorPoolWithScoreSet);
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
                if (!isComplete() || !isValid())
                {
                    /*
                     * recursion could not finish, i.e. the current selection does not allow enough storage pools
                     * remove our selection and retry with the next storage pool
                     */
                    unselectLast();
                    errorReporter.logTrace(
                        "Autoplacer.Selector: Removing StorPool '%s' on Node '%s' from current selection, " +
                            "since we could not complete the selection.",
                        currentSpWithScore.storPool.getName().displayValue,
                        currentSpWithScore.storPool.getNode().getName().displayValue
                    );
                }
            }
        }
    }

    private boolean isComplete()
    {
        return getCurrentState().remainingRscCountToSelect <= 0;
    }

    /**
     * Checks if the current state (once "complete") is also valid. Currently a state can only be invalid if more than 1
     * x-replicas-on-different group is only partially filled
     * That means a call with "--x-replicas-on-different 2 site --place-count 3" should only allow 2 groups, never 3.
     *
     * @return
     */
    private boolean isValid()
    {
        State state = getCurrentState();

        boolean isValid = true;
        for (Entry<String, Map<String, Integer>> entry : state.diffProps.entrySet())
        {
            String propKey = entry.getKey();
            Map<String, Integer> valueAndRemainingCountMap = entry.getValue();
            int partiallyFullGroupCount = 0;
            for (Integer remainingCount : valueAndRemainingCountMap.values())
            {
                if (remainingCount > 0)
                {
                    partiallyFullGroupCount++;
                }
            }

            if (partiallyFullGroupCount > 1)
            {
                isValid = false;
                List<String> groupList = new ArrayList<>();
                for (Entry<String, Integer> valueAndCount : valueAndRemainingCountMap.entrySet())
                {
                    groupList.add(
                        xReplicasOnDiffCount.get(propKey) - valueAndCount.getValue() + "x '" + valueAndCount.getKey() +
                            "'"
                    );
                }
                errorReporter.logTrace(
                    "Autoplacer.Selector: current selection is not valid since %s has %d only partially " +
                        "filled groups: %s",
                    propKey,
                    partiallyFullGroupCount,
                    StringUtils.join(groupList, ", ")

                );
            }
        }

        return isValid;
    }

    private void logNotSelecting(StorPool sp, String reason)
    {
        errorReporter.logTrace(
            "Autoplacer.Selector: cannot add StorPool '%s' on Node '%s' to " +
                "canditate-selection as %s",
            sp.getName().displayValue,
            sp.getNode().getName().displayValue,
            reason
        );
    }

    public boolean isAllowed(StorPool sp) throws AccessDeniedException
    {
        final State curState = getCurrentState();

        boolean isAllowed = checkNode(sp, curState);
        isAllowed &= checkSharedSpName(sp, curState);
        isAllowed &= checkDevProviderKind(sp, curState);
        isAllowed &= checkSameProps(sp, curState);
        isAllowed &= checkDiffProps(sp, curState);
        return isAllowed;
    }

    private boolean checkNode(final StorPool spRef, final State curStateRef)
    {
        boolean isAllowed = !curStateRef.containsNode(spRef.getNode());
        if (!isAllowed)
        {
            logNotSelecting(spRef, "another StorPool was already selected from this node");
        }
        return isAllowed;
    }

    private boolean checkSharedSpName(final StorPool spRef, final State curStateRef)
    {
        boolean isAllowed = !curStateRef.containsSharedSpName(spRef.getSharedStorPoolName());
        if (!isAllowed)
        {
            logNotSelecting(spRef, "another StorPool with the given shared-name was already selected");
        }
        return isAllowed;
    }

    private boolean checkDevProviderKind(final StorPool spRef, final State curStateRef) throws AccessDeniedException
    {
        boolean isAllowed = true;
        DeviceProviderKind candidateDevProvKind = spRef.getDeviceProviderKind();
        Version candidateDrbdVersion = getDrbdVersion(spRef);

        var selectedDevProvKindsToDrbdVersionEntrySet = curStateRef.selectedProviderKindsToDrbdVersions.entrySet();
        for (Entry<DeviceProviderKind, List<Version>> selectedKindAndVer : selectedDevProvKindsToDrbdVersionEntrySet)
        {
            DeviceProviderKind selectedKind = selectedKindAndVer.getKey();
            for (Version selectedDrbdVersion : selectedKindAndVer.getValue())
            {
                if (!DeviceProviderKind.isMixingAllowed(
                    candidateDevProvKind,
                    candidateDrbdVersion,
                    selectedKind,
                    selectedDrbdVersion,
                    allowStorPoolMixing
                ))
                {
                    logNotSelecting(
                        spRef,
                        String.format(
                            "its provider kind (%s) does not match already selected (%s)",
                            candidateDevProvKind.name(),
                            selectedKind.name()
                        )
                    );
                    isAllowed = false;
                }
            }
        }
        return isAllowed;
    }


    private boolean checkSameProps(final StorPool spRef, final State curStateRef) throws AccessDeniedException
    {
        boolean isAllowed = true;
        final ReadOnlyProps nodePropsRef = spRef.getNode().getProps(accessContext);

        Iterator<Map.Entry<String, String>> samePropEntrySetIterator = curStateRef.sameProps.entrySet().iterator();
        while (isAllowed && samePropEntrySetIterator.hasNext())
        {
            Map.Entry<String, String> sameProp = samePropEntrySetIterator.next();
            @Nullable String samePropValue = sameProp.getValue();
            if (samePropValue != null)
            {
                String nodePropValue = nodePropsRef.getProp(sameProp.getKey());
                // if the node does not have the property, do not allow selecting this storage pool
                isAllowed = nodePropValue != null && nodePropValue.equals(samePropValue);
                if (!isAllowed)
                {
                    logNotSelecting(
                        spRef,
                        String.format(
                            "the node has property '%s' set to '%s' while the already " +
                            "selected nodes require the value to be '%s'",
                            sameProp.getKey(),
                            nodePropValue,
                            samePropValue
                        )
                    );
                }
            }
        }
        return isAllowed;
    }


    private boolean checkDiffProps(final StorPool spRef, final State curStateRef) throws AccessDeniedException
    {
        boolean isAllowed = true;

        final ReadOnlyProps nodeProps = spRef.getNode().getProps(accessContext);
        Iterator<Entry<String, Map<String, Integer>>> diffPropEntrySetIt = curStateRef.diffProps.entrySet().iterator();
        while (isAllowed && diffPropEntrySetIt.hasNext())
        {
            Map.Entry<String, Map<String, Integer>> diffProp = diffPropEntrySetIt.next();

            String propKey = diffProp.getKey();
            @Nullable String nodePropValue = nodeProps.getProp(propKey);
            if (nodePropValue != null)
            {
                Map<String, Integer> diffPropValueAndRemainingCount = diffProp.getValue();
                @Nullable Integer remainingCount = diffPropValueAndRemainingCount.get(nodePropValue);
                isAllowed = remainingCount == null || remainingCount > 0;
                if (!isAllowed)
                {
                    final String notSelectingReason;
                    if (remainingCount <= X_REPLICAS_USAGE_NOT_ALLOWED)
                    {
                        notSelectingReason = String.format(
                            "the node has property '%s' set to '%s', but that value is not allowed " +
                                "to be selected at all",
                            propKey,
                            nodePropValue
                        );
                    }
                    else
                    {
                        notSelectingReason = String.format(
                            "the node has property '%s' set to '%s', but that value must " +
                                "not be selected again",
                            propKey,
                            nodePropValue
                        );
                    }
                    logNotSelecting(spRef, notSelectingReason);
                }
            }
        }
        return isAllowed;
    }

    private void select(Autoplacer.StorPoolWithScore currentSpWithScoreRef) throws AccessDeniedException
    {
        final State curState = getCurrentState();
        final StorPool curSp = currentSpWithScoreRef.storPool;
        final ReadOnlyProps nodeProps = curSp.getNode().getProps(accessContext);

        errorReporter.logTrace(
            "Autoplacer.Selector: Adding StorPool '%s' on Node '%s' to current selection",
            currentSpWithScoreRef.storPool.getName().displayValue,
            currentSpWithScoreRef.storPool.getNode().getName().displayValue
        );

        // update same props
        Map<String, String> updatedSameProps = new HashMap<>(curState.sameProps);
        for (Map.Entry<String, String> sameProp : curState.sameProps.entrySet())
        {
            if (sameProp.getValue() == null)
            {
                String key = sameProp.getKey();
                String propValue = nodeProps.getProp(key);
                if (propValue != null)
                {
                    updatedSameProps.put(key, propValue);
                }
            }
        }

        // update diff props
        Map<String, Map<String, Integer>> updatedDiffProps = new HashMap<>(curState.diffProps);
        for (Entry<String, Map<String, Integer>> diffProp : curState.diffProps.entrySet())
        {
            String key = diffProp.getKey();
            Map<String, Integer> valuesCopy = new HashMap<>(diffProp.getValue());
            String propValue = nodeProps.getProp(key);
            if (propValue != null)
            {
                @Nullable Integer remainingCount = valuesCopy.get(propValue);
                if (remainingCount == null)
                {
                    remainingCount = xReplicasOnDiffCount.getOrDefault(key, X_REPLICAS_DFLT_COUNT);
                }
                valuesCopy.put(propValue, remainingCount - 1);
            }
            updatedDiffProps.put(key, valuesCopy);
        }

        selectionStack.push(
            new State(
                add(curState.selectedNodes, curSp.getNode()),
                add(curState.selectedSharedSPNames, curSp.getSharedStorPoolName()),
                add(curState.selectedProviderKindsToDrbdVersions, curSp.getDeviceProviderKind(), getDrbdVersion(curSp)),
                add(curState.selectedStorPoolWithScoreSet, currentSpWithScoreRef),
                curState.remainingRscCountToSelect - 1,
                updatedSameProps,
                updatedDiffProps
            )
        );
    }

    private void unselectLast()
    {
        selectionStack.pop();
    }

    private Version getDrbdVersion(StorPool sp) throws AccessDeniedException
    {
        return getDrbdVersion(sp.getNode());
    }

    private Version getDrbdVersion(Node node) throws AccessDeniedException
    {
        return node.getPeer(accessContext).getExtToolsManager().getVersion(ExtTools.DRBD9_KERNEL);
    }

    private <T> HashSet<T> add(Set<T> unmodifiableSetRef, T additionalElementRef)
    {
        HashSet<T> ret = new HashSet<>(unmodifiableSetRef);
        ret.add(additionalElementRef);
        return ret;
    }

    private <K, V> HashMap<K, List<V>> add(
        Map<K, List<V>> unmodifiableMapRef,
        K additionalKeyRef,
        V additionalValueRef
    )
    {
        HashMap<K, List<V>> ret = new HashMap<>();
        for (Map.Entry<K, List<V>> entry : unmodifiableMapRef.entrySet())
        {
            K key = entry.getKey();
            ArrayList<V> list = new ArrayList<>(entry.getValue());
            if (key.equals(additionalKeyRef))
            {
                list.add(additionalValueRef);
            }
            ret.put(additionalKeyRef, Collections.unmodifiableList(list));
        }

        // DO NOT USE ret.computeIfAbsent(...).add(additionalValueRef);
        // since that will also try to add the value if the entry existed before the .computeIfAbsent, which is not what
        // we want to do here
        ret.computeIfAbsent(
            additionalKeyRef,
            ignored -> Collections.singletonList(additionalValueRef)
        );
        return ret;
    }

    private static class State
    {
        private final Set<Node> selectedNodes;
        private final Set<SharedStorPoolName> selectedSharedSPNames;
        private final Map<DeviceProviderKind, List</* DrbdVersion */ Version>> selectedProviderKindsToDrbdVersions;
        private final Set<Autoplacer.StorPoolWithScore> selectedStorPoolWithScoreSet;
        private final int remainingRscCountToSelect;

        private final HashMap<String, String> sameProps;
        /**
         * <p>
         * Outer key is the PropKey itself.
         * </p>
         * <p>
         * Inner key is the PropValue
         * </p>
         * <p>
         * Inner value is the (decreasing) count of how many times the given PropValue is allowed to be used.
         * </p>
         * <p>
         * If the outer value does not exist, the {@link SelectionManager#xReplicasOnDiffCount} should be queried for
         * the initial inner value.
         * </p>
         */
        private final HashMap</* PropKey */String, Map</* PropValue */String, /* remaining-count */Integer>> diffProps;

        State(
            Collection<Node> selectedNodesRef,
            Collection<SharedStorPoolName> selectedSharedSPNamesRef,
            Map<DeviceProviderKind, List<Version>> selectedProviderKindsToDrbdVersionsRef,
            Collection<StorPoolWithScore> selectedStorPoolWithScoreSetRef,
            int remainingRscCountToSelectRef,
            Map<String, String> samePropsRef,
            Map<String, Map<String, Integer>> diffPropsRef
        )
        {
            selectedNodes = new HashSet<>(selectedNodesRef);
            selectedSharedSPNames = new HashSet<>(selectedSharedSPNamesRef);
            selectedProviderKindsToDrbdVersions = new HashMap<>(selectedProviderKindsToDrbdVersionsRef);
            selectedStorPoolWithScoreSet = new HashSet<>(selectedStorPoolWithScoreSetRef);
            remainingRscCountToSelect = remainingRscCountToSelectRef;

            sameProps = new HashMap<>(samePropsRef);
            diffProps = new HashMap<>(diffPropsRef);
        }

        boolean containsNode(Node nodeRef)
        {
            return selectedNodes.contains(nodeRef);
        }

        boolean containsSharedSpName(SharedStorPoolName sharedSpNameRef)
        {
            return selectedSharedSPNames.contains(sharedSpNameRef);
        }
    }
}
