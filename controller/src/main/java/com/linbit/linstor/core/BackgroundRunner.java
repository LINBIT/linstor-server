package com.linbit.linstor.core;

import com.linbit.ImplementationError;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
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
     * @param nodesToLockRef An optional array of Nodes that will prevent other background fluxes to run concurrently
     *     (at least of the other background fluxes also state their nodesToLock). It is highly advised to use this
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
            if (canRun(runCfgRef, busyNodes, true))
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
            if (canRun(runCfgRef, busyNodes, true))
            {
                runCfgRef.flux = runCfgRef.fluxSupplier.get();
                ret = prepareFlux(runCfgRef);
            }
            else
            {
                /*
                 * we MUST NOT add runCfg into the queue at this point.
                 *
                 * if we add runCfg into the queue here, runCfgRef.delayFluxSink is still only initialized within the
                 * lambda of Flux.create(...). This will happen sometimes later, but we will be already outside of the
                 * synchronized block. There is a quite small time-windows, where runCfgRef.delayFluxSink is still null
                 * (i.e. not initialized because the flux is not being executed yet) but another thread/flux might just
                 * finish with another background task, calling the "finished" method of this class. In that method we
                 * take the next element from the queue (which as a reminder still has not initialized its
                 * runCfg.delayFluxSink) and calls runCfg.runDeferredFlux(), which in turn tries to access
                 * runCfg.delayFluxSink, causing a NullPointerException.
                 *
                 * To avoid this, we add the runCfg only to the queue once our fluxSink is initialized and we still have
                 * to wait/queue.
                 */

                Flux<Boolean> finishedFlux = Flux.create(fluxSink ->
                {
                    synchronized (runQueuesByNode)
                    {
                        runCfgRef.delayFluxSink = fluxSink;
                        if (canRun(runCfgRef, busyNodes, true))
                        {
                            runCfgRef.flux = runCfgRef.fluxSupplier.get();
                            runCfgRef.runDeferredFlux();
                        }
                        else
                        {
                            queue(runCfgRef);
                        }
                    }
                });

                ret = finishedFlux.concatMap(keepGoing ->
                {
                    Flux<T> innerRet;
                    if (Boolean.TRUE.equals(keepGoing))
                    {
                        runCfgRef.flux = runCfgRef.fluxSupplier.get().log();
                        innerRet = prepareFlux(runCfgRef);
                    }
                    else
                    {
                        innerRet = Flux.error(
                            // this is more a placeholder than anything else.
                            // currently 'false' cannot be added into this flux's corresponding fluxSink, only 'true'
                            new ImplementationError("Aborting pending background tasks")
                        );
                    }
                    return innerRet;
                });
            }
        }
        return ret;
    }

    /**
     * Assumes that we are in a synchronized(runQueuesByNode) block
     * If runCfg.background is <code>true</code>, the flux will be queued. Otherwise the postponing deferredFlux will be
     * executed immediately.
     */
    private void start(RunConfig<?> runCfgRef)
    {
        if (runCfgRef.background)
        {
            startBackgroundFlux(runCfgRef);
        }
        else
        {
            runCfgRef.runDeferredFlux();
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
     * Starts the given Flux and adds the RunConfig to all affected queues stating that the corresponding node is busy
     */
    private <T> void startBackgroundFlux(RunConfig<T> runCfgRef)
    {
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
        List<RunConfig<?>> runCfgsToStart = new ArrayList<>();
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
                if (canRun(runCfgToCheck, lockedNodes, true))
                {
                    /*
                     * We MUST NOT execute runCfgRef.runDeferredFlux() (which is done via calling start(...)) while we
                     * are within the synchronized (runQueuesByNode) block.
                     *
                     * Calling runCfg.runDeferredFlux() will put a "true" into the flux-sink that is postponing the
                     * actual flux. However, calling the ".next(true)" will actually also start processing the
                     * flux-step. The flux-step will most likely take some linstor-locks. Although we currently have not
                     * taken any linstor locks, we are within the synchronized block. Since the actual flux likely needs
                     * linstor locks, we will need to wait for those locks. Another thread might already have those
                     * locks and now tries to get into the synchronized block we are currently in (=> deadlock).
                     *
                     * To summarize: The deadlock can occur if one thread has for example a linstor-writelock (in case
                     * of creating a snapshot that might be rscDfnMap.writeLock) and is waiting at the
                     * "synchronized(runQueueByNode)", while the other thread is already in the
                     * "synchronized(runQueueByNode)" block and is trying to call this runCfg.runDeferredFlux which
                     * itself tries to execute the next flux-step which will wait for a rscDfnMap lock.
                     *
                     * While we could solve this problem by starting new threads, we simply move the "start(...)" call
                     * outside of the synchronized block.
                     * However, we MUST "takeLocks(...)" (done via "canRun(..., true)") within the synchronized block to
                     * prevent other race-conditions where too many background tasks are executed concurrently.
                     */
                    runCfgsToStart.add(runCfgToCheck);
                } // we do not need to queue otherwise , since runCfgToCheck is already queued

                // whether or not we started, we add the nodes from runCfgToCheck to lockedNodes to simulate execution
                // to ensure priority is kept
                lockedNodes.addAll(runCfgToCheck.nodesToLock);
            }
        }
        for (RunConfig<?> runCfgToStart : runCfgsToStart)
        {
            start(runCfgToStart);
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
     *
     * @param takeLocksIfPossible
     */
    private boolean canRun(RunConfig<?> runCfgRef, Collection<NodeName> lockedNodes, boolean takeLocksIfPossible)
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
        if (ret && takeLocksIfPossible)
        {
            takeLocks(runCfgRef);
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
                    apiCallResponses
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

        private @Nullable FluxSink<Boolean> delayFluxSink;

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
        }

        /*
         * Usually used by background tasks
         */
        public RunConfig(
            String descriptionRef,
            Flux<T> fluxRef,
            AccessContext accCtxRef,
            @Nullable List<NodeName> nodesToLockRef,
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
            @Nullable List<NodeName> nodesToLockRef,
            boolean backgroundRef
        )
        {
            priority = priorityRef;
            description = descriptionRef;
            fluxSupplier = null;
            flux = fluxRef;
            background = backgroundRef;
            nodesToLock = nodesToLockRef == null ? Collections.emptyList() : nodesToLockRef;

            initializeSubscriberContext(descriptionRef, accCtxRef);
        }

        private void initializeSubscriberContext(String descriptionRef, AccessContext accCtxRef)
        {
            subscriberContext.put(ApiModule.API_CALL_NAME, descriptionRef);
            subscriberContext.put(AccessContext.class, accCtxRef);
        }

        private void runDeferredFlux()
        {
            delayFluxSink.next(true);
            delayFluxSink.complete();
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
