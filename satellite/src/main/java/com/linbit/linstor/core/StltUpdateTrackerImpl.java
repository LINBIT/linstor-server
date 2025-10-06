package com.linbit.linstor.core;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
    public List<Flux<ApiCallRc>> updateData(AtomicUpdateHolder holder)
    {
        List<Flux<ApiCallRc>> ret = new ArrayList<>();
        synchronized (sched)
        {
            // TODO extend for ctrl, nodes, storpools, etc....
            for (Entry<UUID, Resource.ResourceKey> rscEntry : holder.resources.entrySet())
            {
                ret.add(
                    update(
                        cachedUpdates.rscUpdates.computeIfAbsent(
                            rscEntry.getValue(),
                            ignored -> new UpdateNotification(rscEntry.getKey())
                        )
                    )
                );
            }
            for (Entry<UUID, SnapshotDefinition.Key> snapEntry : holder.snapshots.entrySet())
            {
                ret.add(
                    update(
                        cachedUpdates.snapshotUpdates.computeIfAbsent(
                            snapEntry.getValue(),
                            ignored -> new UpdateNotification(snapEntry.getKey())
                        )
                    )
                );
            }
        }
        return ret;
    }

    @Override
    public Flux<ApiCallRc> updateNode(UUID nodeUuid, NodeName name)
    {
        UpdateNotification updateNotification;
        synchronized (sched)
        {
            updateNotification = cachedUpdates.nodeUpdates.computeIfAbsent(
                name,
                ignored -> new UpdateNotification(nodeUuid)
            );
        }
        return update(updateNotification);
    }

    @Override
    public Flux<ApiCallRc> updateResource(
        UUID rscUuid,
        ResourceName resourceName,
        NodeName nodeName
    )
    {
        Resource.ResourceKey resourceKey = new Resource.ResourceKey(nodeName, resourceName);
        UpdateNotification updateNotification;
        synchronized (sched)
        {
            updateNotification = cachedUpdates.rscUpdates.computeIfAbsent(
                resourceKey,
                ignored -> new UpdateNotification(rscUuid)
            );
        }
        return update(updateNotification);
    }

    @Override
    public Flux<ApiCallRc> updateStorPool(UUID storPoolUuid, NodeName nodeNameRef, StorPoolName storPoolName)
    {
        UpdateNotification updateNotification;
        StorPool.Key key = new StorPool.Key(nodeNameRef, storPoolName);
        synchronized (sched)
        {
            updateNotification = cachedUpdates.storPoolUpdates.computeIfAbsent(
                key,
                ignored -> new UpdateNotification(storPoolUuid)
            );
        }
        return update(updateNotification);
    }

    @Override
    public Flux<ApiCallRc> updateSnapshot(
        UUID snapshotUuid,
        ResourceName resourceName,
        SnapshotName snapshotName
    )
    {
        SnapshotDefinition.Key snapshotKey = new SnapshotDefinition.Key(resourceName, snapshotName);
        UpdateNotification updateNotification;
        synchronized (sched)
        {
            updateNotification = cachedUpdates.snapshotUpdates.computeIfAbsent(
                snapshotKey,
                ignored -> new UpdateNotification(snapshotUuid)
            );
        }
        return update(updateNotification);
    }

    @Override
    public Flux<ApiCallRc> updateExternalFile(
        UUID externalFileUuid,
        ExternalFileName externalFileName
    )
    {
        UpdateNotification updateNotification;
        synchronized (sched)
        {
            updateNotification = cachedUpdates.externalFileUpdates.computeIfAbsent(
                externalFileName,
                ignored -> new UpdateNotification(externalFileUuid)
            );
        }
        return update(updateNotification);
    }

    @Override
    public Flux<ApiCallRc> updateS3Remote(
        UUID remoteUuid,
        RemoteName remoteName
    )
    {
        UpdateNotification updateNotification;
        synchronized (sched)
        {
            updateNotification = cachedUpdates.remoteUpdates.computeIfAbsent(
                remoteName,
                ignored -> new UpdateNotification(remoteUuid)
            );
        }
        return update(updateNotification);
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
            while (cachedUpdates.isEmpty() && condFlag.get() && !forceWake.get() && block)
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
        private final @Nullable UUID uuid;

        private final List<FluxSink<ApiCallRc>> responseSinks;

        public UpdateNotification(@Nullable UUID uuidRef)
        {
            uuid = uuidRef;
            responseSinks = new ArrayList<>();
        }

        public @Nullable UUID getUuid()
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
        public final Map<StorPool.Key, UpdateNotification> storPoolUpdates = new TreeMap<>();
        public final Map<SnapshotDefinition.Key, UpdateNotification> snapshotUpdates = new TreeMap<>();
        public final Map<ExternalFileName, UpdateNotification> externalFileUpdates = new TreeMap<>();
        public final Map<RemoteName, UpdateNotification> remoteUpdates = new TreeMap<>();

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
            other.externalFileUpdates.putAll(externalFileUpdates);
            other.remoteUpdates.putAll(remoteUpdates);
        }

        /**
         * Indicates whether the UpdateBundle contains any notifications
         *
         * @return True if the UpdateBundle contains notifications, false otherwise
         */
        public boolean isEmpty()
        {
            return !controllerUpdate.isPresent() && nodeUpdates.isEmpty() &&
                rscUpdates.isEmpty() && storPoolUpdates.isEmpty() && snapshotUpdates.isEmpty() &&
                externalFileUpdates.isEmpty() && remoteUpdates.isEmpty();
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
            externalFileUpdates.clear();
            remoteUpdates.clear();
        }
    }

    public static class AtomicUpdateHolder
    {
        private final Map<UUID, Resource.ResourceKey> resources = new HashMap<>();
        private final Map<UUID, SnapshotDefinition.Key> snapshots = new HashMap<>();

        public void putRsc(UUID uuid, NodeName nodeName, ResourceName rscName)
        {
            resources.put(uuid, new Resource.ResourceKey(nodeName, rscName));
        }

        public void putSnap(UUID uuid, ResourceName rscName, SnapshotName snapName)
        {
            snapshots.put(uuid, new SnapshotDefinition.Key(rscName, snapName));
        }
    }
}
