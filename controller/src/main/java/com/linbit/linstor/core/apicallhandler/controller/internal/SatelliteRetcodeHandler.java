package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A handler that reacts to a specific {@link ApiCallRc} return code arriving from a satellite on
 * the response stream of an in-flight {@code peer.apiCall(...)} (e.g. the response to
 * {@code API_CHANGED_RSC}). Handlers are dispatched centrally by
 * {@link SatelliteRetcodeDispatcher} and registered via Guice multibinder.
 *
 * <p>Each handler claims exactly one {@link ApiCallRc} return code. The expectation is that the
 * retcode is specific enough (e.g. {@code FAIL_PORT_BLOCKED_DRBD_ADJUST}) that the response is
 * invariant across callers — there is one canonical reaction for the event the retcode names.
 *
 * <p>Each handler also declares a {@code CTX} type parameter — a handler-private state object the
 * dispatcher instantiates once per Flux subscription via {@link #newContext()} and threads through
 * every callback ({@link #beforeRetry}, {@link #afterSuccess}, {@link #describeExhaustion}).
 * Handlers that need per-retry-session state (e.g. an accumulator of every port that was tried)
 * store it on the context; stateless handlers can use {@link Void} as {@code CTX} and return
 * {@code null} from {@link #newContext()}.
 *
 * <p>Lifecycle for one event:
 * <ol>
 *   <li>Satellite's response stream emits an {@link ApiCallRc} containing this handler's retcode.</li>
 *   <li>Dispatcher invokes {@link #newContext()} once (the first time the handler matches) and
 *       reuses the returned context for every subsequent callback on the same Flux.</li>
 *   <li>Dispatcher calls {@link #beforeRetry} once between attempts (e.g. to repick a port).</li>
 *   <li>Dispatcher re-subscribes the upstream Flux so {@code peer.apiCall(...)} is re-issued with
 *       fresh state.</li>
 *   <li>On (eventual) success — i.e. once the upstream completes without the signal retcode —
 *       the dispatcher invokes {@link #afterSuccess} as a tail step, but only if at least one
 *       retry was triggered.</li>
 *   <li>If {@link #maxRetries} is reached without success, the dispatcher surfaces a clean
 *       {@code ApiRcException} to the caller — no further hooks are invoked.</li>
 * </ol>
 *
 * @param <CTX> handler-private state object created per Flux subscription
 */
public interface SatelliteRetcodeHandler<CTX>
{
    /**
     * The {@link ApiCallRc} return code this handler claims. Must be unique across all registered
     * handlers.
     */
    long retcode();

    /**
     * Maximum number of retry attempts before the dispatcher gives up. Must be {@code >= 1}.
     */
    int maxRetries();

    /**
     * Returns a fresh context object for a new Flux subscription. Called lazily by the dispatcher
     * on the first matching signal — handlers that don't need state should declare {@code CTX} as
     * {@link Void} and return {@code null}.
     */
    CTX newContext();

    /**
     * Invoked once between attempts — after the satellite has signalled the handler's retcode and
     * before the upstream Flux is re-subscribed. Implementations may take locks via
     * {@code scopeRunner.fluxInTransactionalScope(...)} and mutate state that the next attempt
     * will pick up (e.g. repick a port in {@code DrbdRscData}).
     *
     * <p>DO NOT call updateSatellites in this method. This method is only supposed to modify the state, aka
     * prepare for a retry. Reactor itself is told to literally re-run the previous Flux step (which should
     * have been a {@code peer.apiCall(...)}).</p>
     *
     * @param rsc the resource the failing satellite response was for
     * @param signalRc the full {@link ApiCallRc} as received from the satellite — handlers may
     *     read additional info from {@link ApiCallRc.RcEntry#getObjRefs()} (e.g. blocked ports)
     * @param context the handler's per-Flux state (same instance across all retries on this Flux)
     */
    Mono<Void> beforeRetry(Resource rsc, ApiCallRc signalRc, CTX context);

    /**
     * Invoked after the eventually-successful upstream completion — but only if at least one
     * retry was triggered for this Flux. Useful for side-effects that need to propagate the
     * mutated state to other satellites (e.g. notifying peers about a new DRBD port).
     *
     * <p>The returned Flux is concatenated after the upstream's last emission. Errors here
     * propagate to the caller; in particular, {@link com.linbit.linstor.netcom.PeerNotConnectedException}
     * is handled by the regular {@code updateResource} offline route in the called Flux, so
     * implementations may delegate to standard {@code updateSatellites(...)} without
     * special-casing offline peers.
     *
     * @param rsc the resource the (now-successful) satellite response was for
     * @param context the same context that was passed to every {@link #beforeRetry} call on this
     *     Flux — handlers can read its accumulated state if they want to surface a summary
     */
    Flux<ApiCallRc> afterSuccess(Resource rsc, CTX context);

    /**
     * Builds the user-facing {@link ApiCallRc.RcEntry} surfaced to the client once
     * {@link #maxRetries()} attempts have all been exhausted without resolving the satellite's
     * signal. The dispatcher wraps the returned entry into a {@code RetcodeRetryExhaustedException}.
     *
     * <p>The default implementation produces a generic message — handlers should override when
     * they can produce a more actionable message (e.g. enumerate state accumulated on
     * {@code context}, point at properties to inspect, suggest a fix).
     *
     * @param rsc the resource the satellite kept failing on
     * @param context the same context that was passed to every {@link #beforeRetry} call —
     *     usually carries everything the handler needs to describe what was tried
     * @param attempts the retry budget that was exhausted (== {@link #maxRetries()})
     */
    default ApiCallRc.RcEntry describeExhaustion(Resource rsc, CTX context, int attempts)
    {
        return ApiCallRcImpl.entryBuilder(
            ApiConsts.FAIL_UNKNOWN_ERROR,
            String.format(
                "Satellite kept signalling the same condition for resource '%s' on node '%s' " +
                    "after %d retries.",
                rsc.getResourceDefinition().getName().displayValue,
                rsc.getNode().getName().displayValue,
                attempts
            )
        ).build();
    }
}
