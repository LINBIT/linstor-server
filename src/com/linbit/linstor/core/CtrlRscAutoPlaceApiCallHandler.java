package com.linbit.linstor.core;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.DisklessDriverKind;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.transaction.TransactionMgr;

public class CtrlRscAutoPlaceApiCallHandler extends AbsApiCallHandler
{
    private String currentRscName;

    private final ResourceDefinitionMap rscDfnMap;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final CtrlRscApiCallHandler rscApiCallHandler;

    @Inject
    public CtrlRscAutoPlaceApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtxRef,
        // @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        CtrlObjectFactories objectFactories,
        CtrlRscApiCallHandler rscApiCallHandlerRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.RESOURCE,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        rscApiCallHandler = rscApiCallHandlerRef;
    }

    public ApiCallRc autoPlace(
        String rscNameStr,
        int placeCount,
        String storPoolNameStr,
        List<String> notPlaceWithRscListRef,
        String notPlaceWithRscRegexStr,
        List<String> replicasOnDifferentPropList,
        List<String> replicasOnSamePropList
    )
    {
        // TODO extract this method into an own interface implementation
        // that the controller can choose between different auto-place strategies
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                rscNameStr
            );
        )
        {
            List<String> notPlaceWithRscList = toUpperList(notPlaceWithRscListRef);
            if (notPlaceWithRscRegexStr != null)
            {
                notPlaceWithRscList.addAll(getRscNameUpperStrFromRegex(notPlaceWithRscRegexStr));
            }

            // calculate the estimated size of the given resource
            final long rscSize = calculateResourceDefinitionSize(rscNameStr);

            /*
             * build a map of storage pools
             * * where the user has access to
             * * that have enough free space
             * * that are not diskless
             */
            Map<StorPoolName, List<StorPool>> storPools = storPoolDfnMap.values().stream()
                // filter for user access on storPoolDfn
                .filter(storPoolDfn -> storPoolDfn.getObjProt().queryAccess(peerAccCtx).hasAccess(AccessType.USE))
                .flatMap(this::getStorPoolStream)
                // filter for diskless
                .filter(storPool -> !(getDriverKind(storPool) instanceof DisklessDriverKind))
                // filter for user access on node
                .filter(storPool -> storPool.getNode().getObjProt().queryAccess(peerAccCtx).hasAccess(AccessType.USE))
                // filter for enough free space
                .filter(storPool -> getFreeSpace(storPool).orElse(0L) >= rscSize)
                .collect(Collectors.groupingBy(StorPool::getName));

            filterByStorPoolName(storPoolNameStr, storPools);
            if (storPools.isEmpty())
            {
                failNotEnoughCandidates(storPoolNameStr, rscSize);
            }

            Map<StorPoolName, List<Node>> candidates =
                filterByDoNotPlaceWithResource(notPlaceWithRscList, storPools);
            if (candidates.isEmpty())
            {
                failNotEnoughCandidates(storPoolNameStr, rscSize);
            }

            List<Candidate> candidateList = filterByReplicasOn(
                replicasOnSamePropList,
                replicasOnDifferentPropList,
                candidates,
                this::mostRemainingSpaceStrategy
            );
            if (candidateList.isEmpty())
            {
                failNotEnoughCandidates(storPoolNameStr, rscSize);
            }

            // candidates still can be of arbitrary length. we have to make sure
            // all candidates have the length of placeCount
            candidateList = filterCandidates(candidateList, placeCount, this::mostRemainingSpaceStrategy);
            if (candidateList.isEmpty())
            {
                failNotEnoughCandidates(storPoolNameStr, rscSize);
            }

            // we might have a list of candidates and have to choose.
            Collections.sort(
                candidateList,
                this::mostRemainingSpaceStrategy
            );

            Candidate bestCandidate = candidateList.get(0);

            Map<String, String> rscPropsMap = new TreeMap<>();
            rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, bestCandidate.storPoolName.displayValue);

            for (Node node : bestCandidate.nodes)
            {
                rscApiCallHandler.createResource(
                    node.getName().displayValue,
                    rscNameStr,
                    Collections.emptyList(),
                    rscPropsMap,
                    Collections.emptyList(),
                    false, // createResource api should NOT autoClose the current transaction
                    // we will close it when we are finished with the autoPlace
                    apiCallRc
                );
            }
            reportSuccess(
                "Resource '" + rscNameStr + "' successfully autoplaced on " + placeCount + " nodes",
                "Used storage pool: '" + bestCandidate.storPoolName.displayValue + "'\n" +
                "Used nodes: '" + bestCandidate.nodes.stream()
                    .map(node -> node.getName().displayValue)
                    .collect(Collectors.joining("', '")) + "'"
            );
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(rscNameStr),
                getObjRefs(rscNameStr),
                getVariables(rscNameStr),
                apiCallRc
            );
        }
        return apiCallRc;
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

    private long calculateResourceDefinitionSize(String rscNameStr)
    {
        long size = 0;
        try
        {
            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, true);
            Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(peerAccCtx);
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                size += vlmDfn.getVolumeSize(peerAccCtx);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access " + CtrlRscDfnApiCallHandler.getObjectDescriptionInline(rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return size;
    }

    private Stream<StorPool> getStorPoolStream(StorPoolDefinition storPoolDefinition)
    {
        Stream<StorPool> stream;
        try
        {
            stream = storPoolDefinition.streamStorPools(peerAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "stream storage pools of storage pool definition '" +
                    storPoolDefinition.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        return stream;
    }

    private StorageDriverKind getDriverKind(StorPool storPool)
    {
        StorageDriverKind driverKind;
        try
        {
            driverKind = storPool.getDriverKind(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw asImplError(exc);
        }
        return driverKind;
    }

    private Optional<Long> getFreeSpace(StorPool storPool)
    {
        Optional<Long> freeSpace;
        try
        {
            freeSpace = storPool.getFreeSpace(peerAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "query free space of " + CtrlStorPoolApiCallHandler.getObjectDescriptionInline(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return freeSpace;
    }

    private void filterByStorPoolName(String storPoolNameStr, Map<StorPoolName, List<StorPool>> storPools)
    {
        if (storPoolNameStr != null)
        {
            StorPoolName storPoolName = asStorPoolName(storPoolNameStr);
            storPools.keySet().retainAll(Arrays.asList(storPoolName));
        }
    }

    private Map<StorPoolName, List<Node>> filterByDoNotPlaceWithResource(
        List<String> notPlaceWithRscList, Map<StorPoolName, List<StorPool>> storPools
    ) throws AccessDeniedException
    {
        Map<StorPoolName, List<Node>> candidates;
        // try to consider the "do not place with resource" argument on node level.
        candidates = filterByRscNameStr(storPools, notPlaceWithRscList);
        if (candidates.isEmpty())
        {
            // if that didn't work, try to consider the "do not place with resource" argument on storPool level
            for (Entry<StorPoolName, List<StorPool>> entry : storPools.entrySet())
            {
                // build a list of storPools that have at least one of the "do not place with resource" resources.
                List<StorPool> storPoolsToRemove = new ArrayList<>();
                for (StorPool storPool : entry.getValue())
                {
                    Collection<Volume> volumes = storPool.getVolumes(apiCtx);
                    for (Volume vlm : volumes)
                    {
                        if (notPlaceWithRscList.contains(vlm.getResourceDefinition().getName().value))
                        {
                            storPoolsToRemove.add(storPool);
                            break;
                        }
                    }
                }
                // remove that storPools
                entry.getValue().removeAll(storPoolsToRemove);
            }

            // We already applied the filtering on storPool level. That means we can re-run the
            // filterCandidates with no "do not place with resource" restriction on node-level, as we are
            // already only considering the filtered storPools.
            candidates = filterByRscNameStr(storPools, Collections.emptyList());
        }
        return candidates;
    }

    private List<Candidate> filterByReplicasOn(
        List<String> replicasOnSamePropList,
        List<String> replicasOnDiffParamList,
        Map<StorPoolName, List<Node>> candidatesIn,
        NodeSelectionStrategy nodeSelectionStartegy
    )
    {
        List<Candidate> candidatesOut = new ArrayList<>();
        try
        {
            for (Entry<StorPoolName, List<Node>> candidateEntry : candidatesIn.entrySet())
            {
                Map<BucketId, Collection<Node>> buckets = new HashMap<>();
                buckets.put(new BucketId(), candidateEntry.getValue());

                /*
                 * Example:
                 *
                 * Let's assume that we have in replicasOnSamePropList the node-property-keys "A" and "B"
                 * with values "x","y" and "z" for key "A"
                 * and values "1", "2", "3" for "B"
                 *
                 * In the first iteration we have a map with one entry
                 *  key: ""
                 *  value: all nodes from the current storPoolName
                 * In each iteration we extend the key with the current node's property-value (delimited with ":").
                 * That means after the first iteration we will have 3 buckets ":x", ":y" and ":z"
                 *  (the "" bucket is removed by not adding it to the nextSameBuckets reference and
                 *  overriding the "buckets"-map with nextSameBuckets)
                 * After the second iteration, we will have 9 buckets with the following keys:
                 *  ":x:1"
                 *  ":x:2"
                 *  ":x:3"
                 *  ":y:1"
                 *  ...
                 *  ":z:3"
                 * Every node is only in the list of one of those buckets.
                 */
                for (String samePropKey : replicasOnSamePropList)
                {
                    Map<BucketId, Collection<Node>> nextSameBuckets = new HashMap<>();
                    for (Entry<BucketId, Collection<Node>> bucketEntry : buckets.entrySet())
                    {
                        for (Node bucketEntryNode : bucketEntry.getValue())
                        {
                            BucketId entryNodeId = bucketEntry.getKey().extend(
                                bucketEntryNode.getProps(peerAccCtx).getProp(samePropKey)
                            );
                            Collection<Node> nextSameBucketNodes  = nextSameBuckets.get(entryNodeId);
                            if (nextSameBucketNodes == null)
                            {
                                nextSameBucketNodes = new ArrayList<>();
                                nextSameBuckets.put(entryNodeId, nextSameBucketNodes);
                            }
                            nextSameBucketNodes.add(bucketEntryNode);
                        }
                    }
                    buckets = nextSameBuckets;
                }

                /*
                 * Now we have grouped all nodes by the "same" criteria.
                 * Next, for each bucket, we remove the nodes such that each of the
                 * property-values from "replicasOnDiffParamList"-keys are distinct.
                 *
                 * When two nodes have the same value, we will have to choose which node to delete.
                 * For this, we already have to use a "candidate-pre-selection". This is done by the
                 * functional-interface-argument "nodePreSelectionStartegy"
                 */
                for (String diffPropKey : replicasOnDiffParamList)
                {
                    // although we do not care about the key, we need to iterate over the entrySet
                    // as we will have to call setValue after this loop
                    for (Entry<BucketId, Collection<Node>> bucketEntry : buckets.entrySet())
                    {
                        HashMap<String, Node> usedValues = new HashMap<>();
                        for (Node bucketNode : bucketEntry.getValue())
                        {
                            String nodeValue = bucketNode.getProps(peerAccCtx).getProp(diffPropKey);
                            Node sameValueNode = usedValues.get(nodeValue);

                            Node selectedNode;
                            if (sameValueNode == null)
                            {
                                selectedNode = bucketNode;
                            }
                            else
                            {
                                int cmp = nodeSelectionStartegy.compare(
                                    bucketNode,
                                    sameValueNode,
                                    candidateEntry.getKey()
                                );
                                selectedNode = cmp > 0 ? bucketNode : sameValueNode;
                            }

                            usedValues.put(nodeValue, selectedNode);
                        }
                        bucketEntry.setValue(usedValues.values());
                    }
                }

                /*
                 * Currently we have a list of candidates for the current storPoolName.
                 * As we don't know how many nodes a "valid" candidate has to contain, we
                 * simply emit all of our candidates:)
                 */
                for (Collection<Node> nodeList : buckets.values())
                {
                    candidatesOut.add(
                        new Candidate(
                            candidateEntry.getKey(),
                            new ArrayList<>(nodeList)
                        )
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw asImplError(exc); // should have been thrown long ago
        }
        catch (InvalidKeyException exc)
        {
            throw asExc(
                exc,
                "The property key '" + exc.invalidKey + "' is invalid.",
                ApiConsts.FAIL_INVLD_PROP
            );
        }
        return candidatesOut;
    }

    private List<Candidate> filterCandidates(
        List<Candidate> candidateList,
        int placeCount,
        NodeSelectionStrategy nodeSelectionStartegy
    )
    {
        List<Candidate> filteredCandidateList = new ArrayList<>();
        for (Candidate candidate : candidateList)
        {
            int nodeCount = candidate.nodes.size();
            if (nodeCount >= placeCount)
            {
                if (nodeCount > placeCount)
                {
                    Collections.sort(
                        candidate.nodes,
                        (node1, node2) ->
                            nodeSelectionStartegy.compare(node1, node2, candidate.storPoolName)
                    );
                    filteredCandidateList.add(
                        new Candidate(
                            candidate.storPoolName,
                            candidate.nodes.subList(0, placeCount)
                        )
                    );
                }
                else
                {
                    filteredCandidateList.add(candidate);
                }
            }
        }
        return filteredCandidateList;
    }

    private void failNotEnoughCandidates(String storPoolNameStr, final long rscSize)
    {
        addAnswer(
            "Not enough available nodes",
            null, // cause
            "Not enough nodes fulfilling the following auto-place criteria:\n" +
            " * has a deployed storage pool named '" + storPoolNameStr + "'\n" +
            " * the storage pool '" + storPoolNameStr + "' has to have at least '" +
            rscSize + "' free space\n" +
            " * the current access context has enough privileges to use the node and the storage pool",
            null, // correction.... "you must construct additional servers"
            ApiConsts.FAIL_NOT_ENOUGH_NODES
        );
        throw new ApiCallHandlerFailedException();
    }

    private int mostRemainingSpaceStrategy(Candidate cand1, Candidate cand2)
    {
        // the node-lists are already sorted by their storPools.
        // that means, we only have to compare the freeSpace of the first nodes of cand1 and cand2
        int cmp = 0;
        try
        {
            cmp = Long.compare(
                cand2.nodes.get(0).getStorPool(peerAccCtx, cand2.storPoolName)
                    .getFreeSpace(peerAccCtx).orElse(0L),
                cand1.nodes.get(0).getStorPool(peerAccCtx, cand1.storPoolName)
                    .getFreeSpace(peerAccCtx).orElse(0L)
            );
            // compare(cand2, cand1) so that the candidate with more free space comes before the other
        }
        catch (AccessDeniedException exc)
        {
            // this exception should have been thrown long ago
            throw asImplError(exc);
        }
        return cmp;
    }

    private int mostRemainingSpaceStrategy(Node nodeA, Node nodeB, StorPoolName storPoolName)
    {
        int cmp = 0;
        try
        {
            cmp = Long.compare(
                nodeA.getStorPool(peerAccCtx, storPoolName).getFreeSpace(peerAccCtx).orElse(0L),
                nodeB.getStorPool(peerAccCtx, storPoolName).getFreeSpace(peerAccCtx).orElse(0L)
            );
        }
        catch (AccessDeniedException exc)
        {
            throw asImplError(exc);
        }
        return cmp;
    }

    private Map<StorPoolName, List<Node>> filterByRscNameStr(
        Map<StorPoolName, List<StorPool>> storPools,
        List<String> notPlaceWithRscList
    )
    {
        Map<StorPoolName, List<Node>> ret = new HashMap<>();
        for (Entry<StorPoolName, List<StorPool>> entry: storPools.entrySet())
        {
            List<Node> nodeCandidates = entry.getValue().stream()
                .sorted((sp1, sp2) -> getFreeSpace(sp1).orElse(0L).compareTo(getFreeSpace(sp2).orElse(0L)))
                .map(StorPool::getNode)
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
            hasNoResourceOf = node.streamResources(peerAccCtx)
                .map(rsc -> rsc.getDefinition().getName().value)
                .noneMatch(notPlaceWithRscList::contains);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return hasNoResourceOf;
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String rscNameStr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true,
            getObjRefs(rscNameStr),
            getVariables(rscNameStr)
        );
        currentRscName = rscNameStr;
        return this;
    }

    private Map<String, String> getObjRefs(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        return map;
    }

    private Map<String, String> getVariables(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        return map;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Auto-placing resource: " + currentRscName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentRscName);
    }

    private String getObjectDescriptionInline(String rscNameStr)
    {
        return "auto-placing resource: '" + rscNameStr + "'";
    }

    private class Candidate
    {
        StorPoolName storPoolName;
        List<Node> nodes;

        Candidate(StorPoolName storPoolNameRef, List<Node> nodesRef)
        {
            storPoolName = storPoolNameRef;
            nodes = nodesRef;
        }

        @Override
        public String toString()
        {
            return "Candidate [storPoolName=" + storPoolName + ", nodes=" + nodes + "]";
        }
    }

    @FunctionalInterface
    private interface NodeSelectionStrategy
    {
        int compare(Node nodeA, Node nodeB, StorPoolName storPoolName);
    }

    private class BucketId
    {
        BucketId parent;
        String id;

        /**
         * Root bucket constructor
         */
        BucketId()
        {
        }

        /**
         * Child bucket Constructor
         */
        BucketId(BucketId parentRef, String idRef)
        {
            parent = parentRef;
            id = idRef;
        }

        BucketId extend(String idRef)
        {
            return new BucketId(this, idRef);
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((parent == null) ? 0 : parent.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = false;
            if (obj != null && obj instanceof BucketId)
            {
                BucketId other = (BucketId) obj;
                eq = Objects.equals(other.id, this.id) && Objects.equals(other.parent, this.parent);
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return parent == null ?
                id :
                parent.toString() + ", " + id;
        }
    }
}
