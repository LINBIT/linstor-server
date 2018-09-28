package com.linbit.linstor.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRc;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

/**
 * Tracks update notifications received from a controller
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
class StltUpdateTrackerImpl implements StltUpdateTracker
{
    private final Object sched;

    private final UpdateBundle cachedUpdates;
    private final Scheduler scheduler;

    StltUpdateTrackerImpl(Object schedRef, Scheduler schedulerRef)
    {
        sched = schedRef;
        scheduler = schedulerRef;
        cachedUpdates = new UpdateBundle();
    }

    @Override
    public Flux<ApiCallRc> updateController()
    {
        // we don't care if we override an existing value, we just have to make sure that
        // cachedUpdates.controllerUpdate is NOT null (this will trigger the devMgr to re-generate .res files, etc)
        cachedUpdates.controllerUpdate = Optional.of(new UpdateNotification(null));
        return update(cachedUpdates.controllerUpdate.get());
    }

    @Override
    public Flux<ApiCallRc> updateNode(UUID nodeUuid, NodeName name)
    {
        return update(cachedUpdates.nodeUpdates.computeIfAbsent(name, ignored -> new UpdateNotification(nodeUuid)));
    }

    @Override
    public Flux<ApiCallRc> updateResourceDfn(UUID rscDfnUuid, ResourceName name)
    {
        return update(cachedUpdates.rscDfnUpdates.computeIfAbsent(name, ignored -> new UpdateNotification(rscDfnUuid)));
    }

    @Override
    public Flux<ApiCallRc> updateResource(
        UUID rscUuid,
        ResourceName resourceName,
        NodeName nodeName
    )
    {
        Resource.Key resourceKey = new Resource.Key(resourceName, nodeName);
        return update(cachedUpdates.rscUpdates.computeIfAbsent(
            resourceKey, ignored -> new UpdateNotification(rscUuid)));
    }

    @Override
    public Flux<ApiCallRc> updateStorPool(UUID storPoolUuid, StorPoolName storPoolName)
    {
        return update(cachedUpdates.storPoolUpdates.computeIfAbsent(
            storPoolName, ignored -> new UpdateNotification(storPoolUuid)));
    }

    @Override
    public Flux<ApiCallRc> updateSnapshot(
        UUID snapshotUuid,
        ResourceName resourceName,
        SnapshotName snapshotName
    )
    {
        SnapshotDefinition.Key snapshotKey = new SnapshotDefinition.Key(resourceName, snapshotName);
        return update(cachedUpdates.snapshotUpdates.computeIfAbsent(
            snapshotKey, ignored -> new UpdateNotification(snapshotUuid)));
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

    private Flux<ApiCallRc> update(UpdateNotification updateNotification)
    {
        return Flux
            .<ApiCallRc>create(fluxSink ->
                {
                    synchronized (sched)
                    {
                        updateNotification.addResponseSink(fluxSink);
                        sched.notify();
                    }
                }
            )
            // Handle dispatch responses asynchronously on the main thread pool
            .publishOn(scheduler);
    }

    static class UpdateNotification
    {
        private final UUID uuid;

        private final List<FluxSink<ApiCallRc>> responseSinks;

        UpdateNotification(UUID uuidRef)
        {
            uuid = uuidRef;
            responseSinks = new ArrayList<>();
        }

        public UUID getUuid()
        {
            return uuid;
        }

        public void addResponseSink(FluxSink<ApiCallRc> sink)
        {
            responseSinks.add(sink);
        }

        public List<FluxSink<ApiCallRc>> getResponseSinks()
        {
            return responseSinks;
        }
    }

    /**
     * Groups update notifications and check notifications
     */
    static class UpdateBundle
    {
        Optional<UpdateNotification> controllerUpdate = Optional.empty();
        final Map<NodeName, UpdateNotification> nodeUpdates = new TreeMap<>();
        final Map<ResourceName, UpdateNotification> rscDfnUpdates = new TreeMap<>();
        final Map<Resource.Key, UpdateNotification> rscUpdates = new TreeMap<>();
        final Map<StorPoolName, UpdateNotification> storPoolUpdates = new TreeMap<>();
        final Map<SnapshotDefinition.Key, UpdateNotification> snapshotUpdates = new TreeMap<>();

        /**
         * Copies the update notifications, but not the check notifications, to another UpdateBundle
         * All notifications are cleared from the other UpdateBundle before copying.
         *
         * @param other The UpdateBundle to copy the requests to
         */
        void copyUpdateRequestsTo(UpdateBundle other)
        {
            other.clear();

            other.controllerUpdate = controllerUpdate;
            other.nodeUpdates.putAll(nodeUpdates);
            other.rscDfnUpdates.putAll(rscDfnUpdates);
            other.rscUpdates.putAll(rscUpdates);
            other.storPoolUpdates.putAll(storPoolUpdates);
            other.snapshotUpdates.putAll(snapshotUpdates);
        }

        /**
         * Indicates whether the UpdateBundle contains any notifications
         *
         * @return True if the UpdateBundle contains notifications, false otherwise
         */
        boolean isEmpty()
        {
            return !controllerUpdate.isPresent() && nodeUpdates.isEmpty() && rscDfnUpdates.isEmpty() &&
                rscUpdates.isEmpty() && storPoolUpdates.isEmpty() && snapshotUpdates.isEmpty();
        }

        /**
         * Clears all notifications
         */
        void clear()
        {
            // Clear the collected updates
            controllerUpdate = Optional.empty();
            nodeUpdates.clear();
            rscDfnUpdates.clear();
            rscUpdates.clear();
            storPoolUpdates.clear();
            snapshotUpdates.clear();
        }
    }
}
