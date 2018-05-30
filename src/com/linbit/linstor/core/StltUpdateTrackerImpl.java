package com.linbit.linstor.core;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotId;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolName;

/**
 * Tracks update notifications received from a controller
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
class StltUpdateTrackerImpl implements StltUpdateTracker
{
    private final Object sched;

    private final UpdateBundle cachedUpdates;

    StltUpdateTrackerImpl(Object schedRef)
    {
        sched = schedRef;
        cachedUpdates = new UpdateBundle();
    }

    @Override
    public void updateController(UUID nodeUuid, NodeName name)
    {
        synchronized (sched)
        {
            cachedUpdates.updControllerMap.put(name, nodeUuid);
            sched.notify();
        }
    }

    @Override
    public void updateNode(UUID nodeUuid, NodeName name)
    {
        synchronized (sched)
        {
            cachedUpdates.updNodeMap.put(name, nodeUuid);
            sched.notify();
        }
    }

    @Override
    public void updateResourceDfn(UUID rscDfnUuid, ResourceName name)
    {
        synchronized (sched)
        {
            cachedUpdates.updRscDfnMap.put(name, rscDfnUuid);
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
                Map<NodeName, UUID> nodeSet = cachedUpdates.updRscMap.get(rscName);
                if (nodeSet == null)
                {
                    nodeSet = new TreeMap<>();
                    cachedUpdates.updRscMap.put(rscName, nodeSet);
                }
                nodeSet.putAll(updNodeSet);
                sched.notify();
            }
        }
    }

    @Override
    public void updateStorPool(UUID storPoolUuid, StorPoolName storPoolName)
    {
        synchronized (sched)
        {
            cachedUpdates.updStorPoolMap.put(storPoolName, storPoolUuid);
            sched.notify();
        }
    }

    @Override
    public void updateSnapshot(ResourceName resourceName, UUID snapshotUuid, SnapshotName snapshotName)
    {
        synchronized (sched)
        {
            cachedUpdates.updSnapshotMap.put(new SnapshotId(resourceName, snapshotName), snapshotUuid);
            sched.notify();
        }
    }

    @Override
    public void checkResource(ResourceName name)
    {
        synchronized (sched)
        {
            cachedUpdates.chkRscSet.add(name);
            sched.notify();
        }
    }

    @Override
    public void checkMultipleResources(Set<ResourceName> rscSet)
    {
        synchronized (sched)
        {
            cachedUpdates.chkRscSet.addAll(rscSet);
            sched.notify();
        }
    }

    void collectUpdateNotifications(UpdateBundle updates, AtomicBoolean condFlag, boolean block)
    {
        synchronized (sched)
        {
            // If no updates are queued, wait for updates
            while (cachedUpdates.isEmpty() && !condFlag.get() && block)
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

            cachedUpdates.copyUpdateRequestsTo(updates);
            updates.chkRscSet.addAll(cachedUpdates.chkRscSet);

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
        cachedUpdates.clear();
    }

    /**
     * Groups update notifications and check notifications
     */
    static class UpdateBundle
    {
        final Map<NodeName, UUID> updControllerMap = new TreeMap<>();
        final Map<NodeName, UUID> updNodeMap = new TreeMap<>();
        final Map<ResourceName, UUID> updRscDfnMap = new TreeMap<>();
        final Map<ResourceName, Map<NodeName, UUID>> updRscMap = new TreeMap<>();
        final Map<StorPoolName, UUID> updStorPoolMap = new TreeMap<>();
        final Map<SnapshotId, UUID> updSnapshotMap = new TreeMap<>();
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

            other.updControllerMap.putAll(updControllerMap);
            other.updNodeMap.putAll(updNodeMap);
            other.updRscDfnMap.putAll(updRscDfnMap);
            for (Entry<ResourceName, Map<NodeName, UUID>> entry : updRscMap.entrySet())
            {
                other.updRscMap.put(entry.getKey(), new TreeMap<>(entry.getValue()));
            }
            other.updStorPoolMap.putAll(updStorPoolMap);
            other.updSnapshotMap.putAll(updSnapshotMap);
        }

        /**
         * Indicates whether the UpdateBundle contains any notifications
         *
         * @return True if the UpdateBundle contains notifications, false otherwise
         */
        boolean isEmpty()
        {
            return updControllerMap.isEmpty() && updNodeMap.isEmpty() && updRscDfnMap.isEmpty() &&
                updRscMap.isEmpty() && updStorPoolMap.isEmpty() && updSnapshotMap.isEmpty() && chkRscSet.isEmpty();
        }

        /**
         * Clears all notifications
         */
        void clear()
        {
            // Clear the collected updates
            updControllerMap.clear();
            updNodeMap.clear();
            updRscDfnMap.clear();
            updRscMap.clear();
            updStorPoolMap.clear();
            updSnapshotMap.clear();
            chkRscSet.clear();
        }
    }
}
