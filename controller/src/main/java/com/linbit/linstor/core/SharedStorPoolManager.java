package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@Singleton
public class SharedStorPoolManager extends BaseTransactionObject
{
    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transFactory;

    private final TransactionMap<SharedStorPoolName, TransactionSet<SharedStorPoolManager, Resource>> queue;
    private final TransactionMap<SharedStorPoolName, Node> activeLocks;

    @Inject
    public SharedStorPoolManager(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transFactoryRef,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        transFactory = transFactoryRef;

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
     * Requests all shared locks required for processing the given Resource.
     *
     * @return A set of {@link Resource}s that were waiting for the now acquired lock(s), and are now ready to be sent
     *         to the satellite for processing.<br />
     *
     *         The returned set can be empty, but not null.<br />
     *
     *         An empty set means that the lock could not be acquired. In this case the request is queued. As soon as
     *         the lock is released, the next set of Resources will be processed.
     */
    public Set<Resource> requestSharedLock(Resource rsc)
    {
        errorReporter.logTrace("%s requesting shared lock(s)", rsc);
        Set<Resource> resourcesReadyToProces = new TreeSet<>();
        try
        {
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, sysCtx);
            Set<SharedStorPoolName> sharedNames = getSharedSpNames(storPools);

            boolean granted = true;
            synchronized (activeLocks)
            {
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
                            TransactionSet<SharedStorPoolManager, Resource> spQueue = queue.get(spSharedName);
                            if (
                                spQueue != null && !spQueue.isEmpty() &&
                                    !Objects.equals(
                                        spQueue.iterator().next(),
                                        rsc
                                    )
                            )
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
                    lock(rsc, sharedNames);
                    resourcesReadyToProces.add(rsc);
                }
                else
                {
                    synchronized (queue)
                    {
                        for (SharedStorPoolName spSharedName : sharedNames)
                        {
                            errorReporter.logTrace("Lock(s) %s rejected for %s. Queuing request", sharedNames, rsc);
                            TransactionSet<SharedStorPoolManager, Resource> sharedSpQueue = queue.get(spSharedName);
                            if (sharedSpQueue == null)
                            {
                                sharedSpQueue = transFactory.createTransactionSet(this, new LinkedHashSet<>(), null);
                                queue.put(spSharedName, sharedSpQueue);
                            }
                            sharedSpQueue.add(rsc);
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return resourcesReadyToProces;
    }

    private void lock(Resource rsc, Set<SharedStorPoolName> sharedNames)
    {
        Node node = rsc.getNode();
        for (SharedStorPoolName spSharedName : sharedNames)
        {
            activeLocks.put(spSharedName, node);
        }
        errorReporter.logTrace("Lock(s) %s granted for %s", sharedNames, rsc);
    }

    /**
     * Releases a lock and (if requests are still queued) processes the next request.
     *
     * @param storPools
     *
     * @return A set of {@link Resource}s that were waiting for the now acquired lock(s), and are now ready to be sent
     *         to the satellite for processing.<br />
     *
     *         The returned set can be empty, but not null.<br />
     *
     *         An empty set means either that no lock-requests were queued, or that all Resources waiting for the lock
     *         also require
     *         at least one additional lock and still have to wait.
     */
    public Set<Resource> releaseLock(StorPool... storPools)
    {
        Set<Resource> nextSet = new TreeSet<>();

        try
        {
            Map<SharedStorPoolName, StorPool> sharedSpNames = groupBySharedSpName(Arrays.asList(storPools));
            if (!sharedSpNames.isEmpty())
            {
                synchronized (activeLocks)
                {
                    synchronized (queue)
                    {
                        Set<Resource> resourcesToCheck = new LinkedHashSet<>();
                        errorReporter.logTrace("Releasing shared storPool locks %s", sharedSpNames);
                        for (Entry<SharedStorPoolName, StorPool> entry : sharedSpNames.entrySet())
                        {
                            SharedStorPoolName sharedSpName = entry.getKey();
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

                            TransactionSet<SharedStorPoolManager, Resource> sharedSpQueue = queue.get(sharedSpName);
                            if (sharedSpQueue != null)
                            {
                                resourcesToCheck.addAll(sharedSpQueue);
                            }
                        }

                        Map<SharedStorPoolName, Node> reservedLocks = new HashMap<>();
                        Set<SharedStorPoolName> currentlyAcquiredLock = new HashSet<>();

                        System.out.println("checking resources: " + resourcesToCheck);
                        for (Resource rsc : resourcesToCheck)
                        {
                            System.out.println("  rsc: " + rsc);
                            Set<SharedStorPoolName> requiredLocks = getSharedSpNames(
                                LayerVlmUtils.getStorPools(rsc, sysCtx)
                            );

                            boolean granted = true;
                            for (SharedStorPoolName lock : requiredLocks)
                            {
                                Node reservedByNode = reservedLocks.get(lock);
                                if (
                                    (activeLocks.containsKey(lock) && !currentlyAcquiredLock.contains(lock)) ||
                                    (reservedByNode != null && !Objects.equals(reservedByNode, rsc.getNode()))
                                )
                                {
                                    granted = false;
                                    System.out.println("lock already/still taken: " + lock);
                                    // DO NOT break!
                                }
                                else
                                {
                                    reservedLocks.put(lock, rsc.getNode());
                                }
                            }
                            if (granted)
                            {
                                lock(rsc, requiredLocks);
                                for (SharedStorPoolName lock : requiredLocks)
                                {
                                    queue.get(lock).remove(rsc);
                                }
                                currentlyAcquiredLock.addAll(requiredLocks);
                                nextSet.add(rsc);
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return nextSet;
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
}
