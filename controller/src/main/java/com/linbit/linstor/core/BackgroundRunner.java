package com.linbit.linstor.core;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

@Singleton
public class BackgroundRunner
{
    private final ErrorReporter errorReporter;
    private final AccessContext accCtx;

    private final TreeMap<NodeName, TreeSet<RunConfig<?>>> runQueuesByNode;
    private final TreeSet<NodeName> busyNodes;

    @Inject
    public BackgroundRunner(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef
    )
    {
        errorReporter = errorReporterRef;
        accCtx = apiCtxRef;

        runQueuesByNode = new TreeMap<>();
        busyNodes = new TreeSet<>();
    }

    /**
     * Detach the given flux so that it runs in the background after the current API has terminated.
     *
     * @param operationDescription A description that is used for logging start/end of the flux as well as the
     *     API_CALL_NAME
     * @param backgroundFlux The Flux that will be executed
     * @param nodesToLock An optional array of Nodes that will prevent other background fluxes to run concurrently (at
     *     least of the other background fluxes also state their nodesToLock). It is highly advised to use this
     *     parameter for creating snapshots in background to sequentialize them.
     */
    public <T> void runInBackground(String operationDescription, Flux<T> backgroundFlux, Node... nodesToLockRef)
    {
        List<NodeName> nodeNames = new ArrayList<>();
        for (Node node : nodesToLockRef)
        {
            nodeNames.add(node.getName());
        }
        runInBackground(new RunConfig<>(operationDescription, backgroundFlux, accCtx, nodeNames, true));
    }

    public void runInBackground(RunConfig<?> runCfgRef)
    {
        synchronized (runQueuesByNode)
        {
            if (canRun(runCfgRef, busyNodes))
            {
                startBackgroundFlux(runCfgRef);
            }
            else
            {
                queue(runCfgRef);
            }
        }
    }

    public <T> Flux<T> syncWithBackgroundFluxes(RunConfig<T> runCfgRef)
    {
        Flux<T> ret;
        synchronized (runQueuesByNode)
        {
            if (canRun(runCfgRef, busyNodes))
            {
                takeLocks(runCfgRef);
                runCfgRef.flux = runCfgRef.fluxSupplier.get();
                ret = prepareFlux(runCfgRef);
            }
            else
            {
                queue(runCfgRef);
                ret = Flux.defer(() ->
                {
                    Flux<T> flux;
                    try
                    {
                        runCfgRef.flux = runCfgRef.deferedFluxBlockingQueue.take();
                        flux = prepareFlux(runCfgRef);
                    }
                    catch (InterruptedException exc)
                    {
                        exc.printStackTrace();
                        errorReporter.reportError(
                            exc,
                            accCtx,
                            null,
                            "Exception occurred during " + runCfgRef.description
                        );
                        flux = Flux.empty();
                    }
                    return flux;
                });
            }
        }
        return ret;
    }

    private void start(RunConfig<?> runCfgRef)
    {
        if (runCfgRef.background)
        {
            startBackgroundFlux(runCfgRef);
        }
        else
        {
            startForegroundFlux(runCfgRef);
        }
    }

    /**
     * Assumes that runQueuesByNode lock is taken
     *
     * Prepares the given Flux by adding onSubscribe, onTermiate and other callback handlers.
     * DOES NOT take any locks and DOES NOT subscribe to the given flux.
     */
    private <T> Flux<T> prepareFlux(RunConfig<T> runCfgRef)
    {

        return runCfgRef.flux
            .doOnSubscribe(ignored ->
                errorReporter.logDebug("Background operation " + runCfgRef.description + " start"))
            .doOnTerminate(() ->
            {
                errorReporter.logDebug("Background operation " + runCfgRef.description + " end");
                finished(runCfgRef);
            })
            .contextWrite(Context.of(runCfgRef.subscriberContext));
    }

    /**
     * Assumes that runQueuesByNode lock is taken
     *
     * This method assumes that the given Flux is already subscribed to, but is waiting in the artifically created
     * Flux.defer() method.
     * All this method has to do is to put the actual Flux into the supplier of the artificial Flux.defer() and let the
     * already subscribed Flux continue.
     */
    private <T> void startForegroundFlux(RunConfig<T> runCfgRef)
    {
        takeLocks(runCfgRef);
        runCfgRef.runDeferredFlux();
    }

    /**
     * Assumes that runQueuesByNode lock is taken
     *
     * Starts the given Flux and adds the RunConfig to all affected queues stating that the corresponding node is busy
     */
    private <T> void startBackgroundFlux(RunConfig<T> runCfgRef)
    {
        takeLocks(runCfgRef);
        prepareFlux(runCfgRef)
            .subscribe(
                responses ->
                {
                    reportErrorResponses(runCfgRef.description, responses);
                    if (runCfgRef.subscriptionConsumer != null)
                    {
                        runCfgRef.subscriptionConsumer.accept(responses);
                    }
                },
                exc ->
                {
                    errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "Error in background operation " + runCfgRef.description
                    );
                    if (runCfgRef.subscriptionErrorConsumer != null)
                    {
                        runCfgRef.subscriptionErrorConsumer.accept(exc);
                    }
                }
            );
    }

    private <T> void takeLocks(RunConfig<T> runCfgRef)
    {
        busyNodes.addAll(runCfgRef.nodesToLock);
    }

    /**
     * Removes the given {@link RunConfig} from the queues and starts new {@link RunConfig}s (possibly multiple) that
     * where blocked until now
     */
    private void finished(RunConfig<?> runCfgRef)
    {
        synchronized (runQueuesByNode)
        {
            TreeSet<RunConfig<?>> nextToCheck = new TreeSet<>();
            for (NodeName node : runCfgRef.nodesToLock)
            {
                TreeSet<RunConfig<?>> queue = runQueuesByNode.get(node);
                if (queue != null)
                {
                    queue.remove(runCfgRef);

                    if (!queue.isEmpty())
                    {
                        nextToCheck.add(queue.first());
                    }
                }
            }
            busyNodes.removeAll(runCfgRef.nodesToLock);

            // copy the busyNodes so we can extend lockedNodes for simulation so that we can ensure the priority will be
            // kept
            TreeSet<NodeName> lockedNodes = new TreeSet<>(busyNodes);

            for (RunConfig<?> runCfgToCheck : nextToCheck)
            {
                if (canRun(runCfgToCheck, lockedNodes))
                {
                    start(runCfgToCheck);
                } // we do not need to queue otherwise , since runCfgToCheck is already queued

                // whether or not we started, we add the nodes from runCfgToCheck to lockedNodes to simulate execution
                // to ensure priority is kept
                lockedNodes.addAll(runCfgToCheck.nodesToLock);
            }
        }
    }

    /**
     * Assumes that runQueuesByNode lock is taken
     */
    private void queue(RunConfig<?> runCfgRef)
    {
        for (NodeName node : runCfgRef.nodesToLock)
        {
            runQueuesByNode.computeIfAbsent(node, ignored -> new TreeSet<>())
                .add(runCfgRef);
        }
    }

    /**
     * Assumes that runQueuesByNode lock is taken
     */
    private boolean canRun(RunConfig<?> runCfgRef, Collection<NodeName> lockedNodes)
    {
        boolean ret = true;
        for (NodeName nodeName : runCfgRef.nodesToLock)
        {
            if (lockedNodes.contains(nodeName))
            {
                ret = false;
                break;
            }
        }
        return ret;
    }

    private void reportErrorResponses(String operationDescription, Object responses)
    {
        if (responses instanceof ApiCallRc)
        {
            ApiCallRc apiCallResponses = (ApiCallRc) responses;
            if (ApiRcUtils.isError(apiCallResponses))
            {
                errorReporter.logError(
                    "Error response from background operation %s: %s",
                    operationDescription,
                    apiCallResponses.getEntries()
                        .stream()
                        .map(ApiCallRc.RcEntry::getMessage)
                        .collect(Collectors.joining("; "))
                );
            }
        }
    }

    public static class RunConfig<T> implements Comparable<RunConfig<T>>
    {
        private static final AtomicLong ID_GEN = new AtomicLong(0);

        /** lower value -> higher priority. Default is System.currentTimeMillis(); */
        private final long priority;
        /** since a non-unique priority is not enough for TreeSet, we also introduce a unique id */
        private final long uniqueId = ID_GEN.getAndIncrement();

        private final String description;
        private @Nullable Flux<T> flux;
        private final @Nullable Supplier<Flux<T>> fluxSupplier;
        private final Map<Object, Object> subscriberContext = new HashMap<>();

        private final List<NodeName> nodesToLock;
        private final boolean background;

        private @Nullable Consumer<T> subscriptionConsumer = null;
        private @Nullable Consumer<Throwable> subscriptionErrorConsumer = null;

        private @Nullable ArrayBlockingQueue<Flux<T>> deferedFluxBlockingQueue;


        /*
         * Usually used by foreground tasks
         */
        public RunConfig(
            long priorityRef,
            String descriptionRef,
            Supplier<Flux<T>> fluxSupplierRef,
            AccessContext accCtxRef,
            List<NodeName> nodesToLockRef,
            boolean backgroundRef
        )
        {
            priority = priorityRef;
            description = descriptionRef;
            fluxSupplier = fluxSupplierRef;
            flux = null;
            nodesToLock = nodesToLockRef;
            background = backgroundRef;

            initializeSubscriberContext(descriptionRef, accCtxRef);

            if (!backgroundRef)
            {
                deferedFluxBlockingQueue = new ArrayBlockingQueue<>(1);
            }
            else
            {
                deferedFluxBlockingQueue = null;
            }
        }

        /*
         * Usually used by background tasks
         */
        public RunConfig(
            String descriptionRef,
            Flux<T> fluxRef,
            AccessContext accCtxRef,
            List<NodeName> nodesToLockRef,
            boolean backgroundRef
        )
        {
            this(System.currentTimeMillis(), descriptionRef, fluxRef, accCtxRef, nodesToLockRef, backgroundRef);
        }

        /*
         * Usually used by background tasks
         */
        public RunConfig(
            long priorityRef,
            String descriptionRef,
            Flux<T> fluxRef,
            AccessContext accCtxRef,
            List<NodeName> nodesToLockRef,
            boolean backgroundRef
        )
        {
            priority = priorityRef;
            description = descriptionRef;
            fluxSupplier = null;
            flux = fluxRef;
            background = backgroundRef;
            nodesToLock = nodesToLockRef;

            initializeSubscriberContext(descriptionRef, accCtxRef);

            if (!backgroundRef)
            {
                deferedFluxBlockingQueue = new ArrayBlockingQueue<>(1);
            }
            else
            {
                deferedFluxBlockingQueue = null;
            }
        }

        private void initializeSubscriberContext(String descriptionRef, AccessContext accCtxRef)
        {
            subscriberContext.put(ApiModule.API_CALL_NAME, descriptionRef);
            subscriberContext.put(AccessContext.class, accCtxRef);
        }

        private void runDeferredFlux()
        {
            deferedFluxBlockingQueue.add(fluxSupplier.get());
        }

        public RunConfig<T> putSubscriberContext(Object key, Object value)
        {
            subscriberContext.put(key, value);
            return this;
        }

        public RunConfig<T> setSubscriptionConsumers(
            Consumer<T> subscriptionConsumerRef,
            Consumer<Throwable> subscriptionErrorConsumerRef
        )
        {
            setSubscriptionConsumer(subscriptionConsumerRef);
            setSubscriptionErrorConsumer(subscriptionErrorConsumerRef);
            return this;
        }

        public RunConfig<T> setSubscriptionConsumer(Consumer<T> consumerRef)
        {
            subscriptionConsumer = consumerRef;
            return this;
        }

        public RunConfig<T> setSubscriptionErrorConsumer(Consumer<Throwable> consumerRef)
        {
            subscriptionErrorConsumer = consumerRef;
            return this;
        }

        @Override
        public int compareTo(RunConfig<T> otherRef)
        {
            int cmp = Long.compare(priority, otherRef.priority);
            if (cmp == 0)
            {
                cmp = Long.compare(uniqueId, otherRef.uniqueId);
            }
            return cmp;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(uniqueId);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof RunConfig))
            {
                return false;
            }
            RunConfig<?> other = (RunConfig<?>) obj;
            return uniqueId == other.uniqueId;
        }
    }
}
