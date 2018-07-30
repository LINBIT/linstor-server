package com.linbit.linstor.core;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
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
    public void updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName)
    {
        synchronized (sched)
        {
            Resource.Key resourceKey = new Resource.Key(resourceName, nodeName);
            cachedUpdates.updRscMap.put(resourceKey, rscUuid);
            sched.notify();
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
    public void updateSnapshot(
        UUID snapshotUuid,
        ResourceName resourceName,
        SnapshotName snapshotName
    )
    {
        synchronized (sched)
        {
            cachedUpdates.updSnapshotMap.put(new SnapshotDefinition.Key(resourceName, snapshotName), snapshotUuid);
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
        final Map<Resource.Key, UUID> updRscMap = new TreeMap<>();
        final Map<StorPoolName, UUID> updStorPoolMap = new TreeMap<>();
        final Map<SnapshotDefinition.Key, UUID> updSnapshotMap = new TreeMap<>();

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
            other.updRscMap.putAll(updRscMap);
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
                updRscMap.isEmpty() && updStorPoolMap.isEmpty() && updSnapshotMap.isEmpty();
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
        }
    }
}
