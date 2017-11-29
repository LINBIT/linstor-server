package com.linbit.linstor.core;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;

/**
 * Tracks update notifications received from a controller
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
class StltUpdateTrackerImpl implements StltUpdateTracker
{
    private final Object sched;

    private final Map<ResourceName, UUID> rscDfnCache;
    private final Map<ResourceName, Map<NodeName, UUID>> rscCache;
    private final Map<NodeName, UUID> nodesCache;
    private final Map<StorPoolName, UUID> storPoolCache;
    private final Map<ResourceName, UUID> chkRscCache;

    StltUpdateTrackerImpl(Object schedRef)
    {
        sched = schedRef;
        rscDfnCache = new TreeMap<>();
        rscCache = new TreeMap<>();
        nodesCache = new TreeMap<>();
        storPoolCache = new TreeMap<>();
        chkRscCache = new TreeMap<>();
    }

    @Override
    public void updateNode(UUID nodeUuid, NodeName name)
    {
        synchronized (sched)
        {
            nodesCache.put(name, nodeUuid);
            sched.notify();
        }
    }

    @Override
    public void updateResourceDfn(UUID rscDfnUuid, ResourceName name)
    {
        synchronized (sched)
        {
            rscDfnCache.put(name, rscDfnUuid);
            sched.notify();
        }
    }

    @Override
    public void updateResource(ResourceName rscName, Map<NodeName, UUID> updNodeSet)
    {
        if (!updNodeSet.isEmpty())
        {
            synchronized (sched)
            {
                Map<NodeName, UUID> nodeSet = rscCache.get(rscName);
                if (nodeSet == null)
                {
                    nodeSet = new TreeMap<>();
                    rscCache.put(rscName, nodeSet);
                }
                nodeSet.putAll(updNodeSet);
                sched.notify();
            }
        }
    }

    @Override
    public void updateStorPool(UUID storPoolUuid, StorPoolName name)
    {
        synchronized (sched)
        {
            storPoolCache.put(name, storPoolUuid);
            sched.notify();
        }
    }

    @Override
    public void checkResource(UUID rscUuid, ResourceName name)
    {
        synchronized (sched)
        {
            chkRscCache.put(name, rscUuid);
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
            updates.updNodeMap.putAll(nodesCache);
            updates.updRscDfnMap.putAll(rscDfnCache);
            updates.updRscMap.putAll(rscCache);
            updates.updStorPoolMap.putAll(storPoolCache);
            updates.chkRscMap.putAll(chkRscCache);

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
        final Map<NodeName, UUID> updNodeMap = new TreeMap<>();
        final Map<ResourceName, UUID> updRscDfnMap = new TreeMap<>();
        final Map<ResourceName, Map<NodeName, UUID>> updRscMap = new TreeMap<>();
        final Map<StorPoolName, UUID> updStorPoolMap = new TreeMap<>();
        final Map<ResourceName, UUID> chkRscMap = new TreeMap<>();

        /**
         * Copies the update notifications, but not the check notifications, to another UpdateBundle
         * All notifications are cleared from the other UpdateBundle before copying.
         *
         * @param other The UpdateBundle to copy the requests to
         */
        void copyUpdateRequestsTo(UpdateBundle other)
        {
            other.clear();

            other.updNodeMap.putAll(updNodeMap);
            other.updRscDfnMap.putAll(updRscDfnMap);
            other.updRscMap.putAll(updRscMap);
            other.updStorPoolMap.putAll(updStorPoolMap);
        }

        /**
         * Indicates whether the UpdateBundle contains any notifications
         *
         * @return True if the UpdateBundle contains notifications, false otherwise
         */
        boolean isEmpty()
        {
            return updNodeMap.isEmpty() && updRscDfnMap.isEmpty() && updRscMap.isEmpty() &&
                   updStorPoolMap.isEmpty() && chkRscMap.isEmpty();
        }

        /**
         * Clears all notifications
         */
        void clear()
        {
            // Clear the collected updates
            updNodeMap.clear();
            updRscDfnMap.clear();
            updRscMap.clear();
            updStorPoolMap.clear();
            chkRscMap.clear();
        }
    }
}
