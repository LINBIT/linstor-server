package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Singleton
public class SharedStorPoolManager extends BaseTransactionObject
{
    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transFactory;

    private final TransactionMap<SharedStorPoolName, TransactionSet<Void, Node>> queueByLock;
    private final TransactionMap<Node, TransactionList<Void, SharedStorPoolName>> queueByNode;
    private final TransactionMap<SharedStorPoolName, Node> activeLocksByLock;
    private final TransactionMap<Node, TransactionList<Void, SharedStorPoolName>> activeLocksByNode;

    @Inject
    public SharedStorPoolManager(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        transFactory = transFactoryRef;

        // queue = transFactoryRef.createTransactionSet(this, new LinkedHashSet<>(), null);
        queueByLock = transFactoryRef.createTransactionMap(new TreeMap<>(), null);
        queueByNode = transFactoryRef.createTransactionMap(new TreeMap<>(), null);
        activeLocksByLock = transFactoryRef.createTransactionMap(new TreeMap<>(), null);
        activeLocksByNode = transFactoryRef.createTransactionMap(new TreeMap<>(), null);

        transObjs = Arrays.asList(queueByLock, activeLocksByLock);
    }

    public boolean isActive(StorPool sp)
    {
        boolean ret;
        if (!sp.isShared())
        { // not shared
            ret = true;
        }
        else
        {
            Node activeNode;
            synchronized (activeLocksByLock)
            {
                activeNode = activeLocksByLock.get(sp.getSharedStorPoolName());
            }
            ret = Objects.equals(activeNode, sp.getNode()); // activeNode might be null
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
        try
        {
            for (StorPool sp : LayerVlmUtils.getStorPools(rsc, sysCtx))
            {
                ret &= isActive(sp);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
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
        synchronized (activeLocksByLock)
        {
            errorReporter.logTrace("%s requesting shared lock(s): %s ", node, locks);
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
                synchronized (queueByLock)
                {
                    for (SharedStorPoolName spSharedName : locks)
                    {
                        TransactionSet<Void, Node> spsharedSpQueue = queueByLock.get(spSharedName);
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
                lock(node, locks);
            }
            else
            {
                synchronized (queueByLock)
                {
                    errorReporter.logTrace("at least some locks already taken. Adding to queue");
                    for (SharedStorPoolName spSharedName : locks)
                    {
                        TransactionSet<Void, Node> sharedSpQueue = queueByLock.get(spSharedName);
                        if (sharedSpQueue == null)
                        {
                            sharedSpQueue = transFactory.createVolatileTransactionSet(new LinkedHashSet<>());
                            queueByLock.put(spSharedName, sharedSpQueue);
                        }

                        // we might want to throw an impl error if this put overrides any value (i.e. the key
                        // already existed, which means that the given node already had some requests pending..)
                        queueByNode.put(
                            node,
                            transFactory.createVolatileTransactionPrimitiveList(new ArrayList<>(locks))
                        );
                        sharedSpQueue.add(node);
                    }
                }
            }
        }
        return granted;
    }

    private void lock(Node node, Collection<SharedStorPoolName> locksRef)
    {
        synchronized (activeLocksByLock)
        {
            synchronized (activeLocksByNode)
            {
                for (SharedStorPoolName spSharedName : locksRef)
                {
                    activeLocksByLock.put(spSharedName, node);
                }
                activeLocksByNode.put(
                    node,
                    transFactory.createVolatileTransactionPrimitiveList(new ArrayList<>(locksRef))
                );
            }
        }
        errorReporter.logTrace("Lock(s) %s granted for %s", locksRef, node);
    }

    public void forgetRequests(Node node)
    {
        synchronized (queueByLock)
        {
            for (TransactionSet<Void, Node> queueValues : queueByLock.values())
            {
                queueValues.remove(node);
            }
            queueByNode.remove(node);
        }
    }

    /**
     * Releases the given locks and (if requests are still queued) processes the next request.
     *
     * If no locks are given, this method looks up all currently active locks of the given node.
     *
     * @param storPools
     *
     * @return
     *
     * @return A Map of {@link Node}s that were waiting for the now acquired lock(s), and are now ready to be
     *         sent to the satellite for processing. The value of each entry is the Set of granted locks.
     *         A node is only part of this map, if all of the previously requested locks could be acquired.<br />
     *
     *         An empty map means either that no lock-requests were queued, or that all items waiting for the lock
     *         also require at least one additional lock and still have to wait.
     */
    public Map<Node, Set<SharedStorPoolName>> releaseLocks(Node nodeReleasingLocks)
    {
        Map<Node, Set<SharedStorPoolName>> ret = new TreeMap<>();
        List<SharedStorPoolName> locksToRelease = new ArrayList<>();
        synchronized (activeLocksByNode)
        {

            TransactionList<Void, SharedStorPoolName> activeLocks = activeLocksByNode
                .get(nodeReleasingLocks);
            if (activeLocks != null)
            {
                locksToRelease.addAll(activeLocks);
            }

        }
        if (!locksToRelease.isEmpty())
        {
            synchronized (activeLocksByLock)
            {
                synchronized (queueByLock)
                {
                    // preserve order of next objects
                    Set<Node> nextNodesToCheck = new LinkedHashSet<>();
                    errorReporter.logTrace("Releasing shared storPool locks %s", locksToRelease);
                    for (SharedStorPoolName lock : locksToRelease)
                    {
                        // release the lock
                        Node releasedLockFromNode = activeLocksByLock.remove(lock);
                        if (releasedLockFromNode == null)
                        {
                            throw new ImplementationError("Cannot release shared lock before lock was acquired");
                        }
                        if (!Objects.equals(nodeReleasingLocks, releasedLockFromNode))
                        {
                            throw new ImplementationError(
                                "The shared lock can only be released by the original requester."
                            );
                        }

                        TransactionSet<Void, Node> sharedSpQueue = queueByLock.get(lock);
                        if (sharedSpQueue != null)
                        {
                            // see if any of these waiting objects can now acquire all the required locks
                            nextNodesToCheck.addAll(sharedSpQueue);
                        }
                    }

                    Map<SharedStorPoolName, Node> currentlyAcquiredLockBy = new HashMap<>();

                    for (Node currentNode : nextNodesToCheck)
                    {
                        Set<SharedStorPoolName> requiredLocks = getSharedSpNames(currentNode);

                        boolean granted = true;
                        for (SharedStorPoolName lock : requiredLocks)
                        {
                            // is the lock currently taken
                            if (activeLocksByLock.containsKey(lock))
                            {
                                // Objects.equals would return true if we took this lock just now
                                // (i.e. in a previous iteration of this for loop)
                                if (!Objects.equals(currentlyAcquiredLockBy.get(lock), currentNode))
                                {
                                    granted = false;
                                    break;
                                }
                            }
                            else
                            {
                                Iterator<Node> queueIt = queueByLock.get(lock).iterator();
                                if (queueIt.hasNext() && !Objects.equals(getNode(queueIt.next()), currentNode))
                                {
                                    granted = false;
                                    break;
                                }
                            }
                        }
                        if (granted)
                        {
                            lock(currentNode, requiredLocks);
                            for (SharedStorPoolName lock : requiredLocks)
                            {
                                queueByLock.get(lock).remove(currentNode);
                                currentlyAcquiredLockBy.put(lock, currentNode);
                            }
                            ret.put(currentNode, requiredLocks);
                        }
                    }
                }
            }
        }
        return ret;
    }


    private Node getNode(TransactionObject txObj)
    {
        Node ret;
        if(txObj instanceof Resource)
        {
            ret = ((Resource) txObj).getNode();
        }
        else if (txObj instanceof Snapshot)
        {
            ret = ((Snapshot) txObj).getNode();
        }
        else if (txObj instanceof Node)
        {
            ret = (Node) txObj;
        }
        else if (txObj instanceof StorPool)
        {
            ret = ((StorPool) txObj).getNode();
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
        try
        {
            if (txObj instanceof Resource)
            {
                ret = getSharedSpNames(LayerVlmUtils.getStorPools((Resource) txObj, sysCtx));
            }
            else if (txObj instanceof Snapshot)
            {
                ret = getSharedSpNames(LayerVlmUtils.getStorPools((Snapshot) txObj, sysCtx));
            }
            else if (txObj instanceof Node)
            {
                ret = getSharedSpNames(
                    ((Node) txObj).streamStorPools(sysCtx)
                        .collect(Collectors.toList())
                );
            }
            else if (txObj instanceof StorPool)
            {
                ret = getSharedSpNames(Collections.singleton((StorPool) txObj));
            }
            else
            {
                throw new ImplementationError(
                    "Unknown TransactionObject type - cannot map to Storage Pool"
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    static Set<SharedStorPoolName> getSharedSpNames(Collection<StorPool> storPoolsRef)
    {
        return groupBySharedSpName(storPoolsRef).keySet();
    }

    static Map<SharedStorPoolName, StorPool> groupBySharedSpName(Collection<StorPool> storPools)
    {
        HashMap<SharedStorPoolName, StorPool> ret = new HashMap<>();
        for (StorPool sp : storPools)
        {
            if (sp.isShared())
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
