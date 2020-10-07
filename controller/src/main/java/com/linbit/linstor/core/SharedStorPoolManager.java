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
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Singleton
public class SharedStorPoolManager extends BaseTransactionObject
{
    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transFactory;
    private final Provider<? extends TransactionMgr> transMgrProvider;

    private final TransactionMap<SharedStorPoolName, TransactionSet<Void, TransactionObject>> queue;
    private final TransactionMap<SharedStorPoolName, Node> activeLocks;

    @Inject
    public SharedStorPoolManager(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        transFactory = transFactoryRef;
        transMgrProvider = transMgrProviderRef;

        // queue = transFactoryRef.createTransactionSet(this, new LinkedHashSet<>(), null);
        queue = transFactoryRef.createTransactionMap(new TreeMap<>(), null);
        activeLocks = transFactoryRef.createTransactionMap(new TreeMap<>(), null);

        transObjs = Arrays.asList(queue, activeLocks);
    }

    public boolean isActive(StorPool sp)
    {
        boolean ret;
        SharedStorPoolName sharedSpName = sp.getSharedStorPoolName();
        if (sharedSpName == null || !sharedSpName.isShared())
        { // not shared
            ret = true;
        }
        else
        {
            Node activeNode;
            synchronized (activeLocks)
            {
                activeNode = activeLocks.get(sharedSpName);
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
        synchronized (activeLocks)
        {
            Set<SharedStorPoolName> sharedNames = getSharedSpNames(txObj);
            if (sharedNames.isEmpty())
            {
                errorReporter.logTrace("No locks required for %s", txObj);
            }
            else
            {
                errorReporter.logTrace("%s requesting shared lock(s): %s ", txObj, sharedNames);
                for (SharedStorPoolName spSharedName : sharedNames)
                {
                    if (activeLocks.containsKey(spSharedName))
                    {
                        // lock already taken, reject
                        granted = false;
                        break;
                    }
                }
                if (granted)
                {
                    synchronized (queue)
                    {
                        for (SharedStorPoolName spSharedName : sharedNames)
                        {
                            TransactionSet<Void, TransactionObject> spsharedSpQueue = queue.get(spSharedName);
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
                    lock(getNode(txObj), sharedNames);
                }
                else
                {
                    synchronized (queue)
                    {
                        errorReporter.logTrace("at least some locks already taken. Adding to queue");
                        for (SharedStorPoolName spSharedName : sharedNames)
                        {
                            TransactionSet<Void, TransactionObject> sharedSpQueue = queue.get(spSharedName);
                            if (sharedSpQueue == null)
                            {
                                sharedSpQueue = transFactory.createVolatileTransactionSet(new LinkedHashSet<>());
                                queue.put(spSharedName, sharedSpQueue);
                            }
                            sharedSpQueue.add(txObj);
                        }
                    }
                }
            }
        }
        return granted;
    }

    private void lock(Node node, Set<SharedStorPoolName> sharedNames)
    {
        for (SharedStorPoolName spSharedName : sharedNames)
        {
            activeLocks.put(spSharedName, node);
        }
        errorReporter.logTrace("Lock(s) %s granted for %s", sharedNames, node);
    }

    /**
     * Releases a lock and (if requests are still queued) processes the next request.
     *
     * @param storPools
     *
     * @return A set of {@link Node}s, {@link StorPool}s, {@link Resource}s and/or {@link Snapshot}s that were waiting
     *         for the now acquired lock(s), and are now ready to be sent to the satellite for processing.<br />
     *
     *         Every returned set can be empty, but not null.<br />
     *
     *         An empty set means either that no lock-requests were queued, or that all items waiting for the lock
     *         also require at least one additional lock and still have to wait.
     */
    public UpdateSet releaseLock(StorPool... storPools)
    {
        UpdateSet updateSet = new UpdateSet();

        Map<SharedStorPoolName, StorPool> sharedSpNames = groupBySharedSpName(Arrays.asList(storPools));
        if (!sharedSpNames.isEmpty())
        {
            synchronized (activeLocks)
            {
                synchronized (queue)
                {
                    // preserve order of next objects
                    Set<TransactionObject> nextObjectsToCheck = new LinkedHashSet<>();
                    errorReporter.logTrace("Releasing shared storPool locks %s", sharedSpNames.keySet());
                    for (Entry<SharedStorPoolName, StorPool> entry : sharedSpNames.entrySet())
                    {
                        SharedStorPoolName sharedSpName = entry.getKey();
                        // release the lock
                        Node node = activeLocks.remove(sharedSpName);
                        if (node == null)
                        {
                            throw new ImplementationError("Cannot release shared lock before lock was acquired");
                        }
                        if (!Objects.equals(entry.getValue().getNode(), node))
                        {
                            throw new ImplementationError(
                                "The shared lock can only be released by the original requester."
                            );
                        }

                        TransactionSet<Void, TransactionObject> sharedSpQueue = queue.get(sharedSpName);
                        if (sharedSpQueue != null)
                        {
                            // see if any of these waiting objects can now acquire all the required locks
                            nextObjectsToCheck.addAll(sharedSpQueue);
                        }
                    }

                    Map<SharedStorPoolName, Node> currentlyAcquiredLockBy = new HashMap<>();

                    for (TransactionObject txObj : nextObjectsToCheck)
                    {
                        Set<SharedStorPoolName> requiredLocks = getSharedSpNames(txObj);
                        Node currentNode = getNode(txObj);

                        boolean granted = true;
                        for (SharedStorPoolName lock : requiredLocks)
                        {
                            // is the lock currently taken
                            if (activeLocks.containsKey(lock))
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
                                if (!Objects.equals(getNode(queue.get(lock).iterator().next()), currentNode))
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
                                queue.get(lock).remove(txObj);
                                currentlyAcquiredLockBy.put(lock, currentNode);
                            }
                            updateSet.add(txObj);
                        }
                    }
                }
            }
        }
        return updateSet;
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
            else
                if (txObj instanceof Snapshot)
                {
                    ret = getSharedSpNames(LayerVlmUtils.getStorPools((Snapshot) txObj, sysCtx));
                }
                else
                    if (txObj instanceof Node)
                    {
                        ret = getSharedSpNames(
                            ((Node) txObj).streamStorPools(sysCtx)
                                .collect(Collectors.toList())
                        );
                    }
                    else
                        if (txObj instanceof StorPool)
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

    private Set<SharedStorPoolName> getSharedSpNames(Collection<StorPool> storPoolsRef)
    {
        return groupBySharedSpName(storPoolsRef).keySet();
    }

    private Map<SharedStorPoolName, StorPool> groupBySharedSpName(Collection<StorPool> storPools)
    {
        HashMap<SharedStorPoolName, StorPool> ret = new HashMap<>();
        for (StorPool sp : storPools)
        {
            SharedStorPoolName sharedSpName = sp.getSharedStorPoolName();
            if (sharedSpName != null && sharedSpName.isShared())
            {
                StorPool oldSp = ret.put(sharedSpName, sp);
                if (oldSp != null)
                {
                    throw new ImplementationError("Cannot process more than one storage pool with same shared name");
                }
            }
        }
        return ret;
    }

    public static class UpdateSet
    {
        public final Set<Node> nodesToUpdate;
        public final Set<StorPool> spToUpdate;
        public final Set<Resource> rscsToUpdate;
        public final Set<Snapshot> snapsToUpdate;

        public UpdateSet()
        {
            nodesToUpdate = new TreeSet<>();
            spToUpdate = new TreeSet<>();
            rscsToUpdate = new TreeSet<>();
            snapsToUpdate = new TreeSet<>();
        }

        private void add(TransactionObject txObj)
        {
            if(txObj instanceof Resource)
            {
                rscsToUpdate.add((Resource) txObj);
            }
            else if (txObj instanceof Snapshot)
            {
                snapsToUpdate.add((Snapshot) txObj);
            }
            else if (txObj instanceof Node)
            {
                nodesToUpdate.add((Node) txObj);
            }
            else if (txObj instanceof StorPool)
            {
                spToUpdate.add((StorPool) txObj);
            }
            else
            {
                throw new ImplementationError("Unknown TransactionObject type - cannot add to UpdateSet");
            }
        }
    }
}
