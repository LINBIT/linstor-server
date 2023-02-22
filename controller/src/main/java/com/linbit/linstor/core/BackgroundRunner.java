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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
        runInBackground(new RunConfig<>(operationDescription, backgroundFlux, accCtx, nodeNames));
    }

    public void runInBackground(RunConfig<?> runCfgRef)
    {
        synchronized (runQueuesByNode)
        {
            if (canRun(runCfgRef, busyNodes))
            {
                start(runCfgRef);
            }
            else
            {
                queue(runCfgRef);
            }
        }
    }

    /**
     * Assumes that runQueuesByNode lock is taken
     *
     * Starts the given Flux and adds the RunConfig to all affected queues stating that the corresponding node is busy
     */
    private <T> void start(RunConfig<T> runCfgRef)
    {
        busyNodes.addAll(runCfgRef.nodesToLock);

        runCfgRef.flux
            .doOnSubscribe(ignored ->
                errorReporter.logDebug("Background operation " + runCfgRef.description + " start"))
            .doOnTerminate(() ->
            {
                errorReporter.logDebug("Background operation " + runCfgRef.description + " end");
                finished(runCfgRef);
            })
            .subscriberContext(Context.of(runCfgRef.subscriberContext))
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
                queue.remove(runCfgRef);

                if (!queue.isEmpty())
                {
                    nextToCheck.add(queue.first());
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

    /**
     * Note: this class has a natural ordering that is inconsistent with equals.
     */
    public static class RunConfig<T> implements Comparable<RunConfig<T>>
    {
        private static final AtomicLong ID_GEN = new AtomicLong(0);

        /** lower value -> higher priority. Default is System.currentTimeMillis(); */
        private final long priority;
        /** since a non-unique priority is not enough for TreeSet, we also introduce a unique id */
        private final long uniqueId = ID_GEN.getAndIncrement();

        private final String description;
        private final Flux<T> flux;
        private final Map<Object, Object> subscriberContext;

        private final List<NodeName> nodesToLock;

        public @Nullable Consumer<T> subscriptionConsumer = null;
        public @Nullable Consumer<Throwable> subscriptionErrorConsumer = null;

        public RunConfig(
            String descriptionRef,
            Flux<T> fluxRef,
            AccessContext accCtxRef,
            List<NodeName> nodesToLockRef
        )
        {
            this(System.currentTimeMillis(), descriptionRef, fluxRef, accCtxRef, nodesToLockRef);
        }

        public RunConfig(
            long priorityRef,
            String descriptionRef,
            Flux<T> fluxRef,
            AccessContext accCtxRef,
            List<NodeName> nodesToLockRef
        )
        {
            priority = priorityRef;
            description = descriptionRef;
            flux = fluxRef;
            subscriberContext = new HashMap<>();
            nodesToLock = nodesToLockRef;

            subscriberContext.put(ApiModule.API_CALL_NAME, description);
            subscriberContext.put(AccessContext.class, accCtxRef);
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
