package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.DisklessDriverKind;
import com.linbit.linstor.storage.StorageDriverKind;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CtrlAutoStorPoolSelector
{
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final ResourceDefinitionMap rscDfnMap;
    private final AccessContext peerAccCtx;
    private final AccessContext apiAccCtx;

    @Inject
    public CtrlAutoStorPoolSelector(
        ResourceDefinitionMap rscDfnMapRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        @PeerContext AccessContext peerAccCtxRef,
        @SystemContext AccessContext apiAccCtxRef
    )
    {
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        peerAccCtx = peerAccCtxRef;
        apiAccCtx = apiAccCtxRef;
    }

    public Candidate findBestCandidate(
        final long rscSize,
        final AutoSelectFilterApi selectFilter,
        final NodeSelectionStrategy nodeSelectionStrategy,
        final CandidateSelectionStrategy candidateSelectionStrategy
    )
        throws NotEnoughFreeNodesException, InvalidKeyException
    {
        List<Candidate> candidateList = getCandidateList(
            rscSize,
            selectFilter,
            nodeSelectionStrategy
        );

        if (candidateList.isEmpty())
        {
            failNotEnoughCandidates(selectFilter.getStorPoolNameStr(), rscSize);
        }
        candidateList.sort((c1, c2) ->
            candidateSelectionStrategy.compare(c1, c2, peerAccCtx));
        return candidateList.get(0);
    }

    public List<Candidate> getCandidateList(
        final long rscSize,
        final AutoSelectFilterApi selectFilter,
        final NodeSelectionStrategy nodeSelectionStrategy
    )
        throws InvalidKeyException
    {
        List<String> notPlaceWithRscList = toUpperList(selectFilter.getNotPlaceWithRscList());
        String notPlaceWithRscRegexStr = selectFilter.getNotPlaceWithRscRegex();
        if (notPlaceWithRscRegexStr != null)
        {
            notPlaceWithRscList.addAll(getRscNameUpperStrFromRegex(notPlaceWithRscRegexStr));
        }

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
            .collect(
                Collectors.groupingBy(
                    StorPool::getName,
                    HashMap::new, // enforce HashMap-implementation,
                    Collectors.toList()
                )
            );

        filterByStorPoolName(selectFilter.getStorPoolNameStr(), storPools);

        Map<StorPoolName, List<Node>> candidates =
            filterByDoNotPlaceWithResource(notPlaceWithRscList, storPools);

        // this method already trims the node-list to placeCount.
        List<Candidate> candidateList = filterByReplicasOn(
            selectFilter.getReplicasOnSameList(),
            selectFilter.getReplicasOnDifferentList(),
            candidates,
            nodeSelectionStrategy,
            selectFilter.getPlaceCount()
        );

        // candidates still can be of arbitrary length. we have to make sure
        // all candidates have the length of placeCount
        //        candidateList = filterCandidates(candidateList, placeCount, nodeSelectionStrategy);

        return candidateList;
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
            stream = storPoolDefinition.streamStorPools(peerAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new RuntimeAccessDeniedException(
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
            driverKind = storPool.getDriverKind(apiAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return driverKind;
    }

    private void filterByStorPoolName(String forcedStorPoolName, Map<StorPoolName, List<StorPool>> storPools)
    {
        if (forcedStorPoolName != null)
        {
            // As the statement "new StorPoolName(forcedStorPoolName)" could throw an
            // InvalidNameException (which we want to avoid, as we should then handle the exception
            // but not here, ... things get complicated) (should be better after api-rework :) )
            // Therefore we search manually if we find the displayName somewhere and retain on that
            StorPoolName storPoolName = null;
            for (StorPoolName spn : storPools.keySet())
            {
                if (forcedStorPoolName.equals(spn.displayValue))
                {
                    storPoolName = spn;
                    break;
                }
            }
            if (storPoolName == null)
            {
                storPools.clear();
            }
            else
            {
                storPools.keySet().retainAll(Arrays.asList(storPoolName));
            }
        }
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

    private Optional<Long> getFreeSpace(StorPool storPool)
    {
        Optional<Long> freeSpace;
        if (storPool == null)
        {
            freeSpace = Optional.empty();
        }
        else
        {
            try
            {
                freeSpace = storPool.getFreeSpace(peerAccCtx);
            }
            catch (AccessDeniedException exc)
            {
                throw new RuntimeAccessDeniedException(
                    exc,
                    "query free space of " + CtrlStorPoolApiCallHandler.getObjectDescriptionInline(storPool),
                    ApiConsts.FAIL_ACC_DENIED_STOR_POOL
                );
            }
        }
        return freeSpace;
    }

    private Map<StorPoolName, List<Node>> filterByDoNotPlaceWithResource(
        List<String> notPlaceWithRscList,
        Map<StorPoolName, List<StorPool>> storPools
    )
    {
        Map<StorPoolName, List<Node>> candidates = null;
        try
        {
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
                        Collection<Volume> volumes = storPool.getVolumes(apiAccCtx);
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
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return candidates;
    }

    private List<Candidate> filterByReplicasOn(
        List<String> replicasOnSamePropList,
        List<String> replicasOnDiffParamList,
        Map<StorPoolName, List<Node>> candidatesIn,
        NodeSelectionStrategy nodeSelectionStartegy,
        int placeCount
    )
        throws InvalidKeyException
    {
        List<Candidate> candidatesOut = new ArrayList<>();
        try
        {
            for (Entry<StorPoolName, List<Node>> candidateEntry : candidatesIn.entrySet())
            {
                Map<BucketId, List<Node>> buckets = new HashMap<>();
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
                    Map<BucketId, List<Node>> nextSameBuckets = new HashMap<>();
                    for (Entry<BucketId, List<Node>> bucketEntry : buckets.entrySet())
                    {
                        for (Node bucketEntryNode : bucketEntry.getValue())
                        {
                            BucketId entryNodeId = bucketEntry.getKey().extend(
                                bucketEntryNode.getProps(peerAccCtx).getProp(samePropKey)
                            );
                            List<Node> nextSameBucketNodes  = nextSameBuckets.get(entryNodeId);
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
                 * Sort the nodes within each bucket so that the most preferred nodes are chosen first.
                 */
                for (List<Node> bucketNodes : buckets.values())
                {
                    bucketNodes.sort((node1, node2) -> nodeSelectionStartegy.compare(
                        node1,
                        node2,
                        candidateEntry.getKey(),
                        peerAccCtx
                    ));
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
                    for (Entry<BucketId, List<Node>> bucketEntry : buckets.entrySet())
                    {
                        HashMap<String, Node> usedValues = new HashMap<>();
                        for (Node bucketNode : bucketEntry.getValue())
                        {
                            String nodeValue = bucketNode.getProps(peerAccCtx).getProp(diffPropKey);
                            Node selectedNode = usedValues.get(nodeValue);
                            if (selectedNode == null)
                            {
                                usedValues.put(nodeValue, bucketNode);
                            }
                        }
                        bucketEntry.setValue(new ArrayList<>(usedValues.values()));
                    }
                }

                /*
                 * Currently we have a list of candidates for the current storPoolName.
                 * As we don't know how many nodes a "valid" candidate has to contain, we
                 * simply emit all of our candidates:)
                 */
                for (Collection<Node> nodeList : buckets.values())
                {
                    addCandidate(candidatesOut, candidateEntry.getKey(), nodeList, placeCount);
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc); // should have been thrown long ago
        }
        return candidatesOut;
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
                        .map(node -> getFreeSpace(
                                getStorPoolPrivileged(node, storPoolName)
                            )
                            .orElse(0L)
                        )
                        .min(Long::compare)
                        .orElse(0L)
                )
            );
        }
    }

    private void failNotEnoughCandidates(StorPoolName storPoolName, final long rscSize)
        throws NotEnoughFreeNodesException
    {
        throw new NotEnoughFreeNodesException(
            "Not enough available nodes", // message
            "Not enough nodes fulfilling the following auto-place criteria:\n" +
            (
                storPoolName == null ?
                "" :
                " * has a deployed storage pool named '" + storPoolName + "'\n" +
                " * the storage pool '" + storPoolName + "' has to have at least '" +
                rscSize + "' free space\n"
            ) +
            " * the current access context has enough privileges to use the node and the storage pool", // description
            null, // cause
            null, // correction.... "you must construct additional servers"
            null // details
        );
    }

    public static int mostRemainingSpaceStrategy(Candidate cand1, Candidate cand2, AccessContext accCtx)
    {
        // the node-lists are already sorted by their storPools.
        // that means, we only have to compare the freeSpace of the first nodes of cand1 and cand2
        int cmp = 0;
        try
        {
            // compare the arguments in reverse order so that the candidate with more free space comes first
            cmp = Long.compare(
                cand2.nodes.get(0).getStorPool(accCtx, cand2.storPoolName)
                    .getFreeSpace(accCtx).orElse(0L),
                cand1.nodes.get(0).getStorPool(accCtx, cand1.storPoolName)
                    .getFreeSpace(accCtx).orElse(0L)
            );
        }
        catch (AccessDeniedException exc)
        {
            // this exception should have been thrown long ago
            throw new ImplementationError(exc);
        }
        return cmp;
    }

    public static int mostRemainingSpaceStrategy(
        Node nodeA,
        Node nodeB,
        StorPoolName storPoolName,
        AccessContext accCtx
    )
    {
        int cmp = 0;
        try
        {
            // compare the arguments in reverse order so that the node with more free space comes first
            cmp = Long.compare(
                nodeB.getStorPool(accCtx, storPoolName).getFreeSpace(accCtx).orElse(0L),
                nodeA.getStorPool(accCtx, storPoolName).getFreeSpace(accCtx).orElse(0L)
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
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
            throw new ImplementationError(accDeniedExc);
        }
        return hasNoResourceOf;
    }

    public static class Candidate
    {
        StorPoolName storPoolName;
        List<Node> nodes;
        long sizeAfterDeployment;

        Candidate(
            StorPoolName storPoolNameRef,
            List<Node> nodesRef,
            long sizeAfterDeploymentRef
        )
        {
            storPoolName = storPoolNameRef;
            nodes = nodesRef;
            sizeAfterDeployment = sizeAfterDeploymentRef;
        }

        public StorPoolName getStorPoolName()
        {
            return storPoolName;
        }

        public List<Node> getNodes()
        {
            return nodes;
        }

        public long getSizeAfterDeployment()
        {
            return sizeAfterDeployment;
        }

        @Override
        public String toString()
        {
            return "Candidate [storPoolName=" + storPoolName + ", nodes=" + nodes +
                ", sizeAfterDeployment=" + sizeAfterDeployment + "]";
        }
    }

    @FunctionalInterface
    public interface NodeSelectionStrategy
    {
        int compare(Node nodeA, Node nodeB, StorPoolName storPoolName, AccessContext accCtx);
    }

    @FunctionalInterface
    public interface CandidateSelectionStrategy
    {
        int compare(Candidate candidate1, Candidate candidate2, AccessContext accCtx);
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

    public static class NotEnoughFreeNodesException extends LinStorException
    {
        private static final long serialVersionUID = -4782970173136200037L;

        public NotEnoughFreeNodesException(String message)
        {
            super(message);
        }

        public NotEnoughFreeNodesException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public NotEnoughFreeNodesException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText);
        }

        public NotEnoughFreeNodesException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText,
            Throwable cause
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, cause);
        }
    }

    public class RuntimeAccessDeniedException extends RuntimeException
    {
        private static final long serialVersionUID = 8412239561692378539L;

        private AccessDeniedException exc;
        private String msg;
        private long rc;

        RuntimeAccessDeniedException(
            AccessDeniedException excRef,
            String msgRef,
            long rcRef
        )
        {
            exc = excRef;
            msg = msgRef;
            rc = rcRef;
        }

        public AccessDeniedException getExc()
        {
            return exc;
        }

        public String getMsg()
        {
            return msg;
        }

        public long getRc()
        {
            return rc;
        }
    }
}
