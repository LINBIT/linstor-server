package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Tracks update notifications received from a controller
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
class StltUpdateTrackerImpl implements StltUpdateTracker
{
    private final Object sched;

    private final Set<ResourceName> rscDfnCache;
    private final Map<ResourceName, Set<NodeName>> rscCache;
    private final Set<NodeName> nodesCache;
    private final Set<StorPoolName> storPoolCache;
    private final Set<ResourceName> chkRscCache;

    StltUpdateTrackerImpl(Object schedRef)
    {
        sched = schedRef;
        rscDfnCache = new TreeSet<>();
        rscCache = new TreeMap<>();
        nodesCache = new TreeSet<>();
        storPoolCache = new TreeSet<>();
        chkRscCache = new TreeSet<>();
    }

    public void updateNode(NodeName name)
    {
        synchronized (sched)
        {
            nodesCache.add(name);
            sched.notify();
        }
    }

    public void updateResourceDfn(ResourceName name)
    {
        synchronized (sched)
        {
            rscDfnCache.add(name);
            sched.notify();
        }
    }

    public void updateResource(ResourceName rscName, Set<NodeName> updNodeSet)
    {
        if (!updNodeSet.isEmpty())
        {
            synchronized (sched)
            {
                    Set<NodeName> nodeSet = rscCache.get(rscName);
                    if (nodeSet == null)
                    {
                        nodeSet = new TreeSet<>();
                        rscCache.put(rscName, nodeSet);
                    }
                    nodeSet.addAll(updNodeSet);
                    sched.notify();
            }
        }
    }

    public void updateStorPool(StorPoolName name)
    {
        synchronized (sched)
        {
            storPoolCache.add(name);
            sched.notify();
        }
    }

    public void checkResource(ResourceName name)
    {
        synchronized (sched)
        {
            chkRscCache.add(name);
            sched.notify();
        }
    }

    void collectUpdateNotifications(UpdateBundle updates)
    {
        updates.clear();

        synchronized (sched)
        {
            // If no updates are queued, wait for updates
            if (nodesCache.isEmpty() && rscDfnCache.isEmpty() && rscCache.isEmpty() &&
                storPoolCache.isEmpty() && chkRscCache.isEmpty())
            {
                try
                {
                    sched.wait();
                }
                catch (InterruptedException ignored)
                {
                }
            }
            // Collect all queued updates
            updates.updNodeSet.addAll(nodesCache);
            updates.updRscDfnSet.addAll(rscDfnCache);
            updates.updRscMap.putAll(rscCache);
            updates.updStorPoolSet.addAll(storPoolCache);
            updates.chkRscSet.addAll(chkRscCache);

            // Clear queued updates
            clearImpl();
        }
    }

    void clear()
    {
        synchronized (sched)
        {
            clearImpl();
        }
    }

    // Must hold the scheduler lock ('synchronized (sched)')
    private void clearImpl()
    {
        nodesCache.clear();
        rscDfnCache.clear();
        rscCache.clear();
        storPoolCache.clear();
        chkRscCache.clear();
    }

    /**
     * Groups update notifications and check notifications
     */
    static class UpdateBundle
    {
        final Set<NodeName> updNodeSet = new TreeSet<>();
        final Set<ResourceName> updRscDfnSet = new TreeSet<>();
        final Map<ResourceName, Set<NodeName>> updRscMap = new TreeMap<>();
        final Set<StorPoolName> updStorPoolSet = new TreeSet<>();
        final Set<ResourceName> chkRscSet = new TreeSet<>();

        /**
         * Copies the update notifications, but not the check notifications, to another UpdateBundle
         * All notifications are cleared from the other UpdateBundle before copying.
         *
         * @param other The UpdateBundle to copy the requests to
         */
        void copyUpdateRequestsTo(UpdateBundle other)
        {
            other.clear();

            other.updNodeSet.addAll(updNodeSet);
            other.updRscDfnSet.addAll(updRscDfnSet);
            other.updRscMap.putAll(updRscMap);
            other.updStorPoolSet.addAll(updStorPoolSet);
        }

        /**
         * Indicates whether the UpdateBundle contains any notifications
         *
         * @return True if the UpdateBundle contains notifications, false otherwise
         */
        boolean isEmpty()
        {
            return updNodeSet.isEmpty() && updRscDfnSet.isEmpty() && updRscMap.isEmpty() &&
                   updStorPoolSet.isEmpty() && chkRscSet.isEmpty();
        }

        /**
         * Clears all notifications
         */
        void clear()
        {
            // Clear the collected updates
            updNodeSet.clear();
            updRscDfnSet.clear();
            updRscMap.clear();
            updStorPoolSet.clear();
            chkRscSet.clear();
        }
    }
}
