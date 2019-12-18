package com.linbit.linstor.core;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.SnapshotDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

/**
 * Tracks update notifications received from a controller
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class StltUpdateTrackerImpl implements StltUpdateTracker
{
    private final Object sched;

    private final UpdateBundle cachedUpdates;
    private final Scheduler scheduler;

    public StltUpdateTrackerImpl(Object schedRef, Scheduler schedulerRef)
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
    public Flux<ApiCallRc> updateResource(
        UUID rscUuid,
        ResourceName resourceName,
        NodeName nodeName
    )
    {
        Resource.ResourceKey resourceKey = new Resource.ResourceKey(nodeName, resourceName);
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

    public void collectUpdateNotifications(
        UpdateBundle updates,
        AtomicBoolean condFlag,
        AtomicBoolean forceWake,
        boolean block
    )
    {
        synchronized (sched)
        {
            // If no updates are queued, wait for updates
            while (cachedUpdates.isEmpty() && !condFlag.get() && !forceWake.get() && block)
            {
                try
                {
                    sched.wait();
                }
                catch (InterruptedException ignored)
                {
                }
            }
            forceWake.set(false);
            // Collect all queued updates

            cachedUpdates.copyUpdateRequestsTo(updates);

            // Clear queued updates
            clearImpl();
        }
    }

    @Override
    public boolean isEmpty()
    {
        return cachedUpdates.isEmpty();
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

    public static class UpdateNotification
    {
        private final UUID uuid;

        private final List<FluxSink<ApiCallRc>> responseSinks;

        public UpdateNotification(UUID uuidRef)
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
    public static class UpdateBundle
    {
        public Optional<UpdateNotification> controllerUpdate = Optional.empty();
        public final Map<NodeName, UpdateNotification> nodeUpdates = new TreeMap<>();
        public final Map<Resource.ResourceKey, UpdateNotification> rscUpdates = new TreeMap<>();
        public final Map<StorPoolName, UpdateNotification> storPoolUpdates = new TreeMap<>();
        public final Map<SnapshotDefinition.Key, UpdateNotification> snapshotUpdates = new TreeMap<>();

        /**
         * Copies the update notifications, but not the check notifications, to another UpdateBundle
         * All notifications are cleared from the other UpdateBundle before copying.
         *
         * @param other The UpdateBundle to copy the requests to
         */
        public void copyUpdateRequestsTo(UpdateBundle other)
        {
            other.clear();

            other.controllerUpdate = controllerUpdate;
            other.nodeUpdates.putAll(nodeUpdates);
            other.rscUpdates.putAll(rscUpdates);
            other.storPoolUpdates.putAll(storPoolUpdates);
            other.snapshotUpdates.putAll(snapshotUpdates);
        }

        /**
         * Indicates whether the UpdateBundle contains any notifications
         *
         * @return True if the UpdateBundle contains notifications, false otherwise
         */
        public boolean isEmpty()
        {
            return !controllerUpdate.isPresent() && nodeUpdates.isEmpty() &&
                rscUpdates.isEmpty() && storPoolUpdates.isEmpty() && snapshotUpdates.isEmpty();
        }

        /**
         * Clears all notifications
         */
        public void clear()
        {
            // Clear the collected updates
            controllerUpdate = Optional.empty();
            nodeUpdates.clear();
            rscUpdates.clear();
            storPoolUpdates.clear();
            snapshotUpdates.clear();
        }
    }
}
