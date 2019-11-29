package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class CtrlAutoStorPoolSelector
{
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final ResourceDefinitionMap rscDfnMap;
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext apiAccCtx;

    @Inject
    public CtrlAutoStorPoolSelector(
        ResourceDefinitionMap rscDfnMapRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext apiAccCtxRef
    )
    {
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        peerAccCtx = peerAccCtxRef;
        apiAccCtx = apiAccCtxRef;
    }

    public Map<StorPoolName, List<Node>> listAvailableStorPools()
    {
        /*
         * build a map of storage pools
         * * where the user has access to
         * * that have enough free space
         * * that are not diskless
         */
        HashMap<StorPoolName, List<StorPool>> tmpMap = storPoolDfnMap.values().stream()
            // filter for user access on storPoolDfn
            .filter(storPoolDfn -> storPoolDfn.getObjProt().queryAccess(peerAccCtx.get()).hasAccess(AccessType.USE))
            .flatMap(this::getStorPoolStream)
            // filter for diskless
            .filter(storPool -> storPool.getDeviceProviderKind().hasBackingDevice())
            // filter for user access on node
            .filter(storPool -> storPool.getNode().getObjProt().queryAccess(peerAccCtx.get()).hasAccess(AccessType.USE))
            // filter for node connected
            .filter(storPool -> getPeerPrivileged(storPool).isConnected())
            .collect(
                Collectors.groupingBy(
                    StorPool::getName,
                    HashMap::new, // enforce HashMap-implementation,
                    Collectors.toList()
                )
            );
        HashMap<StorPoolName, List<Node>> candidateMap = new HashMap<>();
        for (Entry<StorPoolName, List<StorPool>> entry : tmpMap.entrySet())
        {
            candidateMap.put(
                entry.getKey(),
                entry.getValue().stream().map(StorPool::getNode).collect(Collectors.toList())
            );
        }

        return candidateMap;
    }

    public List<Candidate> getCandidateList(
        final Map<StorPoolName, List<Node>> availableStorPools,
        final AutoStorPoolSelectorConfig selectFilter,
        final NodeSelectionStrategy nodeSelectionStrategy
    )
    {
        Map<StorPoolName, List<Node>> storPools = availableStorPools;

        storPools = filterByStorPoolName(selectFilter, storPools);
        storPools = filterByLayerStackAndProviders(selectFilter, storPools);
        storPools = filterByDoNotPlaceWithResource(selectFilter, storPools);

        // this method already trims the node-list to placeCount.
        return filterByReplicasOn(selectFilter, storPools, nodeSelectionStrategy);
    }

    private List<String> toUpperList(List<String> list)
    {
        return list.stream()
            .map(String::toUpperCase)
            .collect(Collectors.toList());
    }

    private List<String> getRscNameUpperStrFromRegex(String rscRegexStr)
    {
        Pattern rscRegexPattern = Pattern.compile(
            rscRegexStr,
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        return rscDfnMap.keySet().stream()
            .map(rscName -> rscName.value)
            .filter(rscName -> rscRegexPattern.matcher(rscName).find())
            .collect(Collectors.toList());
    }

    private Stream<StorPool> getStorPoolStream(StorPoolDefinition storPoolDefinition)
    {
        Stream<StorPool> stream;
        try
        {
            stream = storPoolDefinition.streamStorPools(peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "stream storage pools of storage pool definition '" +
                    storPoolDefinition.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        return stream;
    }

    private Map<StorPoolName, List<Node>> filterByStorPoolName(
        final AutoStorPoolSelectorConfig selectFilter,
        Map<StorPoolName, List<Node>> storPools
    )
    {
        Map<StorPoolName, List<Node>> ret = new HashMap<>();
        String forcedStorPoolName = selectFilter.getStorPoolNameStr();
        if (forcedStorPoolName != null)
        {
            StorPoolName storPoolName = LinstorParsingUtils.asStorPoolName(selectFilter.getStorPoolNameStr());
            List<Node> nodes = storPools.get(storPoolName);
            if (nodes == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                    "Storage pool '" + forcedStorPoolName + "' not found"
                ));
            }
            ret.put(storPoolName, nodes); // skip all other entries
        }
        else
        {
            ret.putAll(storPools);
        }
        return ret;
    }

    private StorPool getStorPoolPrivileged(Node node, StorPoolName storPoolName)
    {
        StorPool storPool;
        try
        {
            storPool = node.getStorPool(apiAccCtx, storPoolName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return storPool;
    }

    private Peer getPeerPrivileged(StorPool storPool)
    {
        Peer peer;
        try
        {
            peer = storPool.getNode().getPeer(apiAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return peer;
    }

    private Map<StorPoolName, List<Node>> filterByLayerStackAndProviders(
        final AutoStorPoolSelectorConfig selectFilter,
        Map<StorPoolName, List<Node>> nodes
    )
    {
        try
        {
            List<DeviceLayerKind> layerStackList = selectFilter.getLayerStackList();
            List<DeviceProviderKind> providerList = selectFilter.getProviderList();
            if (providerList.isEmpty())
            {
                providerList = Arrays.asList(DeviceProviderKind.values());
            }

            Set<StorPoolName> emptyStorPoolNames = new HashSet<>();
            for (Entry<StorPoolName, List<Node>> entry : nodes.entrySet())
            {
                StorPoolName storPoolName = entry.getKey();
                List<Node> nodeList = entry.getValue();

                Set<Node> storPoolsOfTheseNodesNotSupportingLayersOrProviders = new HashSet<>();
                for (Node node : nodeList)
                {
                    StorPool storPool = node.getStorPool(apiAccCtx, storPoolName);
                    if (!node.getPeer(apiAccCtx).getExtToolsManager().getSupportedLayers().containsAll(layerStackList))
                    {
                        storPoolsOfTheseNodesNotSupportingLayersOrProviders.add(node);
                    }
                    if (!providerList.contains(storPool.getDeviceProviderKind()))
                    {
                        storPoolsOfTheseNodesNotSupportingLayersOrProviders.add(node);
                    }
                }
                nodeList.removeAll(storPoolsOfTheseNodesNotSupportingLayersOrProviders);
                if (nodeList.isEmpty())
                {
                    emptyStorPoolNames.add(entry.getKey());
                }
            }
            for (StorPoolName storPoolName : emptyStorPoolNames)
            {
                nodes.remove(storPoolName);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

        return nodes;
    }

    private Map<StorPoolName, List<Node>> filterByDoNotPlaceWithResource(
        final AutoStorPoolSelectorConfig selectFilter,
        Map<StorPoolName, List<Node>> nodes
    )
    {
        List<String> notPlaceWithRscList = new ArrayList<>(toUpperList(selectFilter.getNotPlaceWithRscList()));
        String notPlaceWithRscRegexStr = selectFilter.getNotPlaceWithRscRegex();
        if (notPlaceWithRscRegexStr != null)
        {
            notPlaceWithRscList.addAll(getRscNameUpperStrFromRegex(notPlaceWithRscRegexStr));
        }

        Map<StorPoolName, List<Node>> candidates = null;
        try
        {
            // try to consider the "do not place with resource" argument on node level.
            candidates = filterByRscNameStr(nodes, notPlaceWithRscList);
            if (candidates.isEmpty())
            {
                // if that didn't work, try to consider the "do not place with resource" argument on storPool level
                for (Entry<StorPoolName, List<Node>> entry : nodes.entrySet())
                {
                    // build a list of storPools that have at least one of the "do not place with resource" resources.
                    List<Node> nodesToRemove = new ArrayList<>();
                    for (Node node : entry.getValue())
                    {
                        Collection<VlmProviderObject> volumes = node
                            .getStorPool(apiAccCtx, entry.getKey())
                            .getVolumes(apiAccCtx);
                        for (VlmProviderObject vlm : volumes)
                        {
                            if (notPlaceWithRscList.contains(vlm.getVolume().getResourceDefinition().getName().value))
                            {
                                nodesToRemove.add(node);
                                break;
                            }
                        }
                    }
                    // remove that storPools
                    entry.getValue().removeAll(nodesToRemove);
                }

                // We already applied the filtering on storPool level. That means we can re-run the
                // filterCandidates with no "do not place with resource" restriction on node-level, as we are
                // already only considering the filtered storPools.
                candidates = filterByRscNameStr(nodes, Collections.emptyList());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return candidates;
    }

    /**
     * Parses a property value tuple 'RackId=2' into its key and value part.
     * Property values could also omit the value part.
     *
     * @param propEntry Key value string
     * @return Tuple containing the key and if provided the value.
     */
    private Tuple2<String, Optional<String>> parsePropTuple(final String propEntry)
    {
        String key = propEntry;
        String value = null;
        int equalPos = propEntry.indexOf('=');
        if (equalPos >= 0)
        {
            key = propEntry.substring(0, equalPos);
            value = propEntry.substring(equalPos + 1);
        }

        return Tuples.of(key, Optional.ofNullable(value));
    }

    /*
     * We can NOT return a Map<StorPoolName, List<Node>> here anymore, as we might have multiple
     * node-lists per storPoolName. For example: if one of our map entry-values contains 10 nodes,
     * where 5 of them have property "A"="1" and the others have "A"="2", with a placeCount = 5 and
     * a "replicasOnSamePropList" containing "A".
     * As a result of this filter-method, we will emit 2 candidates for the given storPoolName.
     *
     * That means we have 2 choices. Either return a List<Candidate> or a
     * Map<StorPoolName, List<List<Node>>>.
     */
    private List<Candidate> filterByReplicasOn(
        final AutoStorPoolSelectorConfig selectFilterRef,
        Map<StorPoolName, List<Node>> candidatesRef,
        NodeSelectionStrategy nodeSelectionStartegyRef
    )
    {
        List<Candidate> ret = new ArrayList<>();
        List<Node> nodesRepOnSame = new ArrayList<>();
        List<Node> nodesRepOnDiff = new ArrayList<>();

        List<String> repOnSameFilter = selectFilterRef.getReplicasOnSameList();
        List<String> repOnDiffFilter = selectFilterRef.getReplicasOnDifferentList();

        try
        {
            for (Entry<StorPoolName, List<Node>> candidateEntry : candidatesRef.entrySet())
            {
                Comparator<Node> nodeComparator =
                    nodeSelectionStartegyRef.makeComparator(candidateEntry.getKey(), peerAccCtx.get());

                List<Node> nodesRepOn = new ArrayList<>();
                if (!repOnSameFilter.isEmpty() || !repOnDiffFilter.isEmpty())
                {
                    // Gather the prop values for the props that need to be the same
                    for (Node candidateNode : candidateEntry.getValue())
                    {
                        /* 1. replicas-on-same */

                        // filter nodes that have the same value of a given aux property or a specified value
                        Map<String, String> props = new HashMap<>();
                        for (String propFilterEntry : repOnSameFilter)
                        {
                            Tuple2<String, Optional<String>> propFilterTuple = parsePropTuple(propFilterEntry);
                            String propFilterKey = propFilterTuple.getT1();
                            Optional<String> propFilterVal = propFilterTuple.getT2();
                            String nodePropVal = candidateNode.getProps(peerAccCtx.get()).getProp(propFilterKey);

                            boolean hasPrefPropVal = propFilterVal.isPresent();

                            if (!hasPrefPropVal && nodePropVal != null ||
                                hasPrefPropVal && propFilterVal.get().equals(nodePropVal)
                            )
                            {
                                props.put(propFilterKey, nodePropVal);
                            }
                        }

                        // don't add nodes that haven't all specified replicas on same properties
                        if (props.size() == repOnSameFilter.size())
                        {
                            nodesRepOnSame.add(candidateNode);
                        }

                        /* 2. replicas-on-different */

                        // filter nodes that have a different value of a given aux property or miss a specified value
                        for (String propFilterEntry : repOnDiffFilter)
                        {
                            Tuple2<String, Optional<String>> propFilterTuple = parsePropTuple(propFilterEntry);
                            String propFilterKey = propFilterTuple.getT1();
                            String propFilterVal = propFilterTuple.getT2().isPresent() ?
                                propFilterTuple.getT2().get() : null;

                            String nodePropVal = candidateNode.getProps(peerAccCtx.get()).getProp(propFilterKey);

                            boolean hasNodePropVal = false;
                            Node nodeToRemove = null;
                            for (Node filteredNode : nodesRepOnDiff)
                            {
                                hasNodePropVal = hasNodePropVal(filteredNode, propFilterKey, nodePropVal);
                                if (hasNodePropVal)
                                {
                                    if (nodeComparator.compare(candidateNode, filteredNode) > 0)
                                    {
                                        nodeToRemove = filteredNode;
                                    }
                                    // only one such node can be found as we do not add nodes with same property values
                                    break;
                                }
                            }

                            /*
                            add node to the filtered list if at least one of the following conditions are fulfilled:
                                * current candidate node lacks the whole property
                                * current candidate node is a better choice a previously filtered one with the same
                                  property value
                                * specific value for the filter is given and
                                  the current candidate node does not have a property with this value
                                * specific value for the filter is not given and
                                  the current candidate node does not have the same value of the given property
                                  as another already filtered node
                            */
                            if (nodePropVal == null ||
                                nodesRepOnDiff.remove(nodeToRemove) ||
                                propFilterVal != null && !propFilterVal.equals(nodePropVal) ||
                                propFilterVal == null && !hasNodePropVal
                            )
                            {
                                nodesRepOnDiff.add(candidateNode);
                            }
                        }
                    }

                    // sort the nodes so that the most preferred nodes are chosen first
                    nodesRepOnSame.sort(nodeComparator.reversed());

                    // make sure that all other nodes in the list have the same value
                    // by removing nodes with a different one
                    Map<String, String> prefPropValsMap = new HashMap<>();
                    List<Node> nodesToRemove = new ArrayList<>();
                    for (Node nodeRepOnSame : nodesRepOnSame)
                    {
                        for (String propEntrySame : repOnSameFilter)
                        {
                            Tuple2<String, Optional<String>> propTuple = parsePropTuple(propEntrySame);
                            String propKey = propTuple.getT1();
                            Optional<String> propVal = propTuple.getT2();

                            if (!propVal.isPresent() && !prefPropValsMap.containsKey(propKey))
                            {
                                prefPropValsMap.put(
                                    propKey,
                                    nodeRepOnSame.getProps(peerAccCtx.get()).getProp(propKey)
                                );
                            }
                        }

                        for (Entry<String, String> prefPropValsEntry : prefPropValsMap.entrySet())
                        {
                            if (!nodeRepOnSame.getProps(peerAccCtx.get()).getProp(prefPropValsEntry.getKey())
                                .equals(prefPropValsEntry.getValue()))
                            {
                                nodesToRemove.add(nodeRepOnSame);
                                break;
                            }
                        }
                    }
                    nodesRepOnSame.removeAll(nodesToRemove);

                    // if both filters are present check for intersections of the filtered nodes first
                    if (!repOnSameFilter.isEmpty() && !repOnDiffFilter.isEmpty())
                    {
                        for (Node node : nodesRepOnSame)
                        {
                            if (nodesRepOnDiff.contains(node))
                            {
                                nodesRepOn.add(node);
                            }
                        }
                    }
                    else
                    {
                        nodesRepOn = repOnSameFilter.isEmpty() ? nodesRepOnDiff : nodesRepOnSame;
                    }
                }
                else
                {
                    // make sure that nodes not corresponding to the selection strategy are removed from the result
                    nodesRepOn = candidateEntry.getValue().stream()
                        .sorted(nodeComparator.reversed())
                        .collect(Collectors.toCollection(ArrayList::new));
                }


                // add filtered candidates
                addCandidate(
                    ret,
                    candidateEntry.getKey(),
                    nodesRepOn,
                    selectFilterRef.getPlaceCount()
                );
            }
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_PROP,
                "The property key '" + invalidKeyExc.invalidKey + "' is invalid."
            ), invalidKeyExc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc); // should have been thrown long ago
        }
        return ret;
    }

    private boolean hasNodePropVal(Node node, String propKey, String nodePropVal)
    {
        boolean propValExists = false;
        if (nodePropVal != null)
        {
            try
            {
                propValExists = node.getProps(peerAccCtx.get()).getProp(propKey).equals(nodePropVal);
            }
            catch (AccessDeniedException exc)
            {
                // do not add
            }
        }
        return propValExists;
    }

    private void addCandidate(
        List<Candidate> targetList,
        StorPoolName storPoolName,
        Collection<Node> nodeListRef,
        int nodeCount
    )
    {
        if (nodeListRef.size() >= nodeCount)
        {
            List<Node> nodeList = new ArrayList<>(nodeListRef).subList(0, nodeCount);

            targetList.add(
                new Candidate(
                    storPoolName,
                    nodeList,
                    nodeList.stream()
                        .allMatch(node ->
                            getStorPoolPrivileged(node, storPoolName).getDeviceProviderKind().usesThinProvisioning())
                )
            );
        }
    }

    private Map<StorPoolName, List<Node>> filterByRscNameStr(
        Map<StorPoolName, List<Node>> nodes,
        List<String> notPlaceWithRscList
    )
    {
        Map<StorPoolName, List<Node>> ret = new HashMap<>();
        for (Entry<StorPoolName, List<Node>> entry: nodes.entrySet())
        {
            List<Node> nodeCandidates = entry.getValue().stream()
                .filter(node -> hasNoResourceOf(node, notPlaceWithRscList))
                .collect(Collectors.toList());

            if (!nodeCandidates.isEmpty())
            {
                ret.put(entry.getKey(), nodeCandidates);
            }
        }
        return ret;
    }

    private boolean hasNoResourceOf(Node node, List<String> notPlaceWithRscList)
    {
        boolean hasNoResourceOf = false;
        try
        {
            hasNoResourceOf = node.streamResources(peerAccCtx.get())
                .map(rsc -> rsc.getDefinition().getName().value)
                .noneMatch(notPlaceWithRscList::contains);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return hasNoResourceOf;
    }

    public static class Candidate
    {
        final StorPoolName storPoolName;
        final List<Node> nodes;
        final boolean allThin;

        Candidate(
            StorPoolName storPoolNameRef,
            List<Node> nodesRef,
            boolean allThinRef
        )
        {
            storPoolName = storPoolNameRef;
            nodes = nodesRef;
            allThin = allThinRef;
        }

        public StorPoolName getStorPoolName()
        {
            return storPoolName;
        }

        public List<Node> getNodes()
        {
            return nodes;
        }

        public boolean allThin()
        {
            return allThin;
        }

        @Override
        public String toString()
        {
            return "Candidate [storPoolName=" + storPoolName + ", nodes=" + nodes + "]";
        }
    }

    public static class AutoStorPoolSelectorConfig
    {
        private final int placeCount;
        private final List<String> replicasOnDifferentList;
        private final List<String> replicasOnSameList;
        private final String notPlaceWithRscRegex;
        private final List<String> notPlaceWithRscList;
        private final String storPoolNameStr;
        private final List<DeviceLayerKind> layerStackList;
        private final List<DeviceProviderKind> providerList;

        public AutoStorPoolSelectorConfig(
            int placeCountRef,
            List<String> replicasOnDifferentListRef,
            List<String> replicasOnSameListRef,
            String notPlaceWithRscRegexRef,
            List<String> notPlaceWithRscListRef,
            String storPoolNameStrRef,
            List<DeviceLayerKind> layerStackRef,
            List<DeviceProviderKind> providerListRef
        )
        {
            placeCount = placeCountRef;
            replicasOnDifferentList = replicasOnDifferentListRef;
            replicasOnSameList = replicasOnSameListRef;
            notPlaceWithRscRegex = notPlaceWithRscRegexRef;
            notPlaceWithRscList = notPlaceWithRscListRef;
            storPoolNameStr = storPoolNameStrRef;
            layerStackList = layerStackRef;
            providerList = providerListRef;
        }

        public int getPlaceCount()
        {
            return placeCount;
        }

        public List<String> getReplicasOnDifferentList()
        {
            return replicasOnDifferentList;
        }

        public List<String> getReplicasOnSameList()
        {
            return replicasOnSameList;
        }

        public String getNotPlaceWithRscRegex()
        {
            return notPlaceWithRscRegex;
        }

        public List<String> getNotPlaceWithRscList()
        {
            return notPlaceWithRscList;
        }

        public String getStorPoolNameStr()
        {
            return storPoolNameStr;
        }

        public List<DeviceLayerKind> getLayerStackList()
        {
            return layerStackList;
        }

        public List<DeviceProviderKind> getProviderList()
        {
            return providerList;
        }
    }

    @FunctionalInterface
    public interface NodeSelectionStrategy
    {
        /**
         * @return A comparator for nodes where the preferred node has the greater value as defined by
         * {@link Comparator#compare(Object, Object)}.
         */
        Comparator<Node> makeComparator(StorPoolName storPoolName, AccessContext accCtx);
    }
}
