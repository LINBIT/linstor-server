package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Singleton
public class SharedStorPoolManager
{
    private final ErrorReporter errorReporter;

    /*
     * CAUTION: when synchroizing on the following maps, make sure to keep the
     * order of the variable declarations to prevent deadlocks
     */
    private final TreeMap<SharedStorPoolName, LinkedHashSet<NodeName>> queueByLock;
    private final TreeMap<SharedStorPoolName, NodeName> activeLocksByLock;
    private final TreeMap<NodeName, ArrayList<SharedStorPoolName>> activeLocksByNode;

    @Inject
    public SharedStorPoolManager(
        ErrorReporter errorReporterRef
    )
    {
        errorReporter = errorReporterRef;

        queueByLock = new TreeMap<>();
        activeLocksByLock = new TreeMap<>();
        activeLocksByNode = new TreeMap<>();
    }

    public boolean isActive(StorPool sp)
    {
        boolean ret;
        if (!sp.usesLinstorLocking())
        { // no LINSTOR lock needed
            ret = true;
        }
        else
        {
            @Nullable NodeName activeNodeName;
            synchronized (activeLocksByLock)
            {
                activeNodeName = activeLocksByLock.get(sp.getSharedStorPoolName());
            }
            ret = Objects.equals(activeNodeName, sp.getNode().getName()); // activeNodeName might be null
        }
        return ret;
    }

    // public Set<StorPool> getAcitveStorPools(Node node)
    // {
    // throw new ImplementationError("not implemented yet");
    // }

    public boolean isActive(Resource rsc)
    {
        boolean ret = true;
        for (StorPool sp : LayerVlmUtils.getStorPools(rsc))
        {
            ret &= isActive(sp);
        }
        return ret;
    }

    /**
     * Requests all shared locks (if any) required for processing the given Resource.
     *
     * @return True if the lock could be acquired. False otherwise.
     */
    public boolean requestSharedLock(TransactionObject txObj)
    {
        boolean granted = true;
        synchronized (activeLocksByLock)
        {
            Set<SharedStorPoolName> sharedNames = getSharedSpNames(txObj);
            if (sharedNames.isEmpty())
            {
                errorReporter.logTrace("No locks required for %s", txObj);
            }
            else
            {
                errorReporter.logTrace("%s requesting shared lock(s): %s ", txObj, sharedNames);
                granted = requestSharedLocks(getNode(txObj), sharedNames);
            }
        }
        return granted;
    }

    public boolean requestSharedLocks(Node node, Collection<SharedStorPoolName> locks)
    {
        boolean granted = true;
        synchronized (queueByLock)
        {
            NodeName nodeName = node.getKey();
            synchronized (activeLocksByLock)
            {
                errorReporter.logTrace("%s requesting shared lock(s): %s ", nodeName, locks);
                for (SharedStorPoolName spSharedName : locks)
                {
                    if (activeLocksByLock.containsKey(spSharedName))
                    {
                        // lock already taken, reject
                        granted = false;
                        break;
                    }
                }
                if (granted)
                {
                    for (SharedStorPoolName spSharedName : locks)
                    {
                        @Nullable LinkedHashSet<NodeName> spsharedSpQueue = queueByLock.get(spSharedName);
                        if (spsharedSpQueue != null && !spsharedSpQueue.isEmpty())
                        {
                            /*
                             * the requesting resource is not the next. We have to delay the requesting resource
                             * as the "next" resource is waiting for this lock (+ other lock(s)).
                             *
                             * If we would grant this current request, the other resource might wait indefinitely
                             * long for all locks.
                             */
                            granted = false;
                            break;
                        }
                    }
                }
            }
            if (granted)
            {
                lock(nodeName, locks);
            }
            else
            {
                errorReporter.logTrace("at least some locks already taken. Adding to queue");
                for (SharedStorPoolName spSharedName : locks)
                {
                    queueByLock.computeIfAbsent(spSharedName, ignored -> new LinkedHashSet<>())
                        .add(nodeName);
                }
            }
        }
        return granted;
    }

    private void lock(NodeName nodeName, Collection<SharedStorPoolName> locksRef)
    {
        synchronized (activeLocksByLock)
        {
            synchronized (activeLocksByNode)
            {
                for (SharedStorPoolName spSharedName : locksRef)
                {
                    activeLocksByLock.put(spSharedName, nodeName);
                }
                activeLocksByNode.put(nodeName, new ArrayList<>(locksRef));
            }
        }
        errorReporter.logTrace("Lock(s) %s granted for %s", locksRef, nodeName);
    }

    public void forgetRequests(NodeName nodeName)
    {
        synchronized (queueByLock)
        {
            for (LinkedHashSet<NodeName> queueValues : queueByLock.values())
            {
                queueValues.remove(nodeName);
            }
        }
    }

    /**
     * <p>Releases the given locks and (if requests are still queued) processes the next request.</p>
     *
     * <p>Note: The returned map of {@link NodeName}s may refer to nodes that no longer exist / were deleted.
     * The caller has to load the {@link Node} from the returned {@code NodeName} and check the {@code Node} for
     * {@code null} as well as for {@code .isDeleted()}!</p>
     *
     * @return A Map of {@link NodeName}s that were waiting for the now acquired lock(s), and are now ready to be
     *         sent to the satellite for processing. The value of each entry is the Set of granted locks.
     *         A node is only part of this map, if all of the previously requested locks could be acquired.<br />
     *
     *         An empty map means either that no lock-requests were queued, or that all items waiting for the lock
     *         also require at least one additional lock and still have to wait.
     */
    public Map<NodeName, Set<SharedStorPoolName>> releaseLocks(NodeName nodeNameReleasingLocks)
    {
        Map<NodeName, Set<SharedStorPoolName>> ret = new TreeMap<>();
        List<SharedStorPoolName> locksToRelease = new ArrayList<>();
        synchronized (queueByLock)
        {
            synchronized (activeLocksByLock)
            {
                synchronized (activeLocksByNode)
                {
                    @Nullable ArrayList<SharedStorPoolName> activeLocks = activeLocksByNode.get(nodeNameReleasingLocks);
                    if (activeLocks != null)
                    {
                        locksToRelease.addAll(activeLocks);
                    }

                    if (!locksToRelease.isEmpty())
                    {
                        // preserve order of next objects
                        Set<NodeName> nextNodeNamesToCheck = new LinkedHashSet<>();
                        errorReporter.logTrace("Releasing shared storPool locks %s", locksToRelease);
                        for (SharedStorPoolName lock : locksToRelease)
                        {
                            // release the lock
                            @Nullable NodeName releasedLockFromNodeName = activeLocksByLock.remove(lock);
                            if (releasedLockFromNodeName == null)
                            {
                                throw new ImplementationError(
                                    "Cannot release shared lock before lock was acquired"
                                );
                            }
                            if (!Objects.equals(nodeNameReleasingLocks, releasedLockFromNodeName))
                            {
                                throw new ImplementationError(
                                    "The shared lock can only be released by the original requester."
                                );
                            }

                            @Nullable LinkedHashSet<NodeName> sharedSpQueue = queueByLock.get(lock);
                            if (sharedSpQueue != null)
                            {
                                // see if any of these waiting objects can now acquire all the required locks
                                nextNodeNamesToCheck.addAll(sharedSpQueue);
                            }
                        }
                        activeLocksByNode.remove(nodeNameReleasingLocks); // all locks released

                        Map<SharedStorPoolName, NodeName> currentlyAcquiredLockBy = new HashMap<>();

                        for (NodeName currentNodeName : nextNodeNamesToCheck)
                        {
                            Set<SharedStorPoolName> requiredLocks = getRequestedLocks(currentNodeName);

                            boolean granted = true;
                            for (SharedStorPoolName lock : requiredLocks)
                            {
                                // is the lock currently taken
                                if (activeLocksByLock.containsKey(lock))
                                {
                                    // Objects.equals would return true if we took this lock just now
                                    // (i.e. in a previous iteration of this for loop)
                                    if (!Objects.equals(currentlyAcquiredLockBy.get(lock), currentNodeName))
                                    {
                                        granted = false;
                                        break;
                                    }
                                }
                                else
                                {
                                    @Nullable LinkedHashSet<NodeName> queue = queueByLock.get(lock);
                                    if (queue != null)
                                    {
                                        Iterator<NodeName> queueIt = queue.iterator();
                                        if (queueIt.hasNext() && !Objects.equals(queueIt.next(), currentNodeName))
                                        {
                                            granted = false;
                                            break;
                                        }
                                    }
                                }
                            }
                            if (granted)
                            {
                                lock(currentNodeName, requiredLocks);
                                for (SharedStorPoolName lock : requiredLocks)
                                {
                                    @Nullable LinkedHashSet<NodeName> queue = queueByLock.get(lock);
                                    if (queue != null)
                                    {
                                        queue.remove(currentNodeName);
                                    }
                                    currentlyAcquiredLockBy.put(lock, currentNodeName);
                                }
                                ret.put(currentNodeName, requiredLocks);
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

    private Set<SharedStorPoolName> getRequestedLocks(NodeName currentNodeNameRef)
    {
        Set<SharedStorPoolName> ret = new HashSet<>();
        synchronized (queueByLock)
        {
            for (Entry<SharedStorPoolName, LinkedHashSet<NodeName>> entry : queueByLock.entrySet())
            {
                if (entry.getValue().contains(currentNodeNameRef))
                {
                    ret.add(entry.getKey());
                }
            }
        }
        return ret;
    }

    public boolean hasNodeActiveLocks(Node node)
    {
        boolean hasLocks;
        synchronized (activeLocksByNode)
        {
            @Nullable ArrayList<SharedStorPoolName> activeLocks = activeLocksByNode.get(node.getKey());
            hasLocks = activeLocks != null && !activeLocks.isEmpty();
        }
        return hasLocks;
    }

    private Node getNode(TransactionObject txObj)
    {
        Node ret;
        if (txObj instanceof Resource rsc)
        {
            ret = rsc.getNode();
        }
        else
        if (txObj instanceof Snapshot snap)
        {
            ret = snap.getNode();
        }
        else
        if (txObj instanceof Node node)
        {
            ret = node;
        }
        else
        if (txObj instanceof StorPool storPool)
        {
            ret = storPool.getNode();
        }
        else
        {
            throw new ImplementationError("Unknown TransactionObject type - cannot map to Node");
        }
        return ret;
    }

    private Set<SharedStorPoolName> getSharedSpNames(TransactionObject txObj)
    {
        Set<SharedStorPoolName> ret;
        if (txObj instanceof Resource rsc)
        {
            ret = getSharedSpNames(LayerVlmUtils.getStorPools(rsc));
        }
        else
        if (txObj instanceof Snapshot snap)
        {
            ret = getSharedSpNames(LayerVlmUtils.getStorPools(snap));
        }
        else
        if (txObj instanceof Node node)
        {
            ret = getSharedSpNames(
                node.streamStorPools()
                    .collect(Collectors.toList())
            );
        }
        else
        if (txObj instanceof StorPool storPool)
        {
            ret = getSharedSpNames(Collections.singleton(storPool));
        }
        else
        {
            throw new ImplementationError(
                "Unknown TransactionObject type - cannot map to Storage Pool"
            );
        }
        return ret;
    }

    /**
     * Returns the lock names for the given storage pools, i.e. the shared storage pool names of the
     * pools whose access LINSTOR itself has to serialize. Externally locked pools (e.g. lvmlockd)
     * share their data just as well, but take no part in LINSTOR's locking.
     */
    private static Set<SharedStorPoolName> getSharedSpNames(Collection<StorPool> storPoolsRef)
    {
        return groupBySharedSpName(storPoolsRef).keySet();
    }

    private static Map<SharedStorPoolName, StorPool> groupBySharedSpName(Collection<StorPool> storPools)
    {
        HashMap<SharedStorPoolName, StorPool> ret = new HashMap<>();
        for (StorPool sp : storPools)
        {
            if (sp.usesLinstorLocking())
            {
                StorPool oldSp = ret.put(sp.getSharedStorPoolName(), sp);
                if (oldSp != null)
                {
                    throw new ImplementationError("Cannot process more than one storage pool with same shared name");
                }
            }
        }
        return ret;
    }
}
