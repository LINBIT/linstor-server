package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.Retry.RetrySignal;

/**
 * Central dispatch point for {@link SatelliteRetcodeHandler}s. Applied via
 * {@code .transform(dispatcher.forResource(rsc))} after every
 * {@code peer.apiCall(...).map(deserializeApiCallRc)} site in
 * {@link CtrlSatelliteUpdateCaller}.
 *
 * <p>Pass-through semantics: if no handler is registered for any retcode emitted by the
 * upstream, the dispatcher is a no-op. This keeps the mechanism safe to wire in everywhere
 * before any concrete handlers exist.
 *
 * <p>Errors raised by the dispatcher itself ({@link SatelliteSignalException} during retry,
 * {@link RetcodeRetryExhaustedException} after exhaustion) can be recognized by callers via
 * {@link #isDispatcherError(Throwable)} so that unrelated error-handling (e.g. the
 * {@code retryResourceTaskProvider} re-queue) can skip them.
 */
@Singleton
public class SatelliteRetcodeDispatcher
{
    private final ErrorReporter errorReporter;
    private final Map<Long, SatelliteRetcodeHandler<?>> handlersByRetcode;

    @Inject
    @SuppressWarnings("rawtypes")
    public SatelliteRetcodeDispatcher(ErrorReporter errorReporterRef, Set<SatelliteRetcodeHandler> handlersRef)
    {
        errorReporter = errorReporterRef;
        // The Guice multibinder gives us a raw Set; iterate as wildcard since each element is
        // some SatelliteRetcodeHandler<X> with X erased at runtime.
        handlersByRetcode = new HashMap<>();
        for (SatelliteRetcodeHandler<?> handler : handlersRef)
        {
            SatelliteRetcodeHandler<?> previous = handlersByRetcode.put(handler.retcode(), handler);
            if (previous != null)
            {
                throw new IllegalStateException(
                    "Two SatelliteRetcodeHandler instances claim retcode 0x" +
                        Long.toHexString(handler.retcode()) + ": " +
                        previous.getClass().getName() + " and " + handler.getClass().getName()
                );
            }
        }
    }

    /**
     * Returns an operator (suitable for {@link Flux#transform(Function)}) that wraps an
     * upstream satellite-response Flux with retry-on-signal semantics for the given resource.
     *
     * <p>Each call to this method returns a fresh operator with independent retry state, so the
     * operator must be applied per-Flux (typically per per-node Flux inside
     * {@code updateResource} and siblings).
     */
    public Function<Flux<ApiCallRc>, Flux<ApiCallRc>> forResource(Resource rsc)
    {
        // Per-Flux state. Both refs are populated lazily on the first matching signal — `triggered`
        // tells the post-retry concatWith whether to invoke afterSuccess at all, and `context`
        // holds the handler-private state object that gets threaded through every callback.
        // Reactor serializes signals within a subscription so neither ref needs synchronization.
        AtomicReference<SatelliteRetcodeHandler<?>> triggered = new AtomicReference<>();
        AtomicReference<Object> context = new AtomicReference<>();

        return sourceFlux -> sourceFlux
            .onErrorResume(ApiRcException.class, apiRcExc ->
            {
                // CommonMessageProcessor#handleAnswer pre-scans every apiCall response: if any
                // entry has the MASK_ERROR bit, the whole response is converted into an
                // ApiRcException and pushed via peer.apiCallError() instead of
                // peer.apiCallAnswer(). That means a FAIL-mask retcode never reaches us as a
                // `next` emission and never gets a chance to hit the .handle(...) below. Unwrap
                // here only when the rc matches a registered handler; otherwise re-raise the
                // original exception so unrelated failures keep their FAIL severity and reach
                // the client unchanged.
                return match(apiRcExc.getApiCallRc()) != null ?
                    Flux.just(apiRcExc.getApiCallRc()) :
                    Flux.error(apiRcExc);
            })
            .<ApiCallRc>handle((apiCallRc, sink) ->
            {
                // we need to use .handle here since other methods of Flux (like map or filter) do not allow
                // us to change a "succeeding" ApiCallRc into a flux-error. But that is exactly what we want to
                // do here and what .handle does allow us to do.

                @Nullable SatelliteRetcodeHandler<?> handler = match(apiCallRc);
                if (handler == null)
                {
                    // ApiCallRc looks fine - nothing to do, just forward this value.
                    sink.next(apiCallRc);
                }
                else
                {
                    // ApiCallRc contains special code that needs to be dealt with. Emit Flux-error instead. The very
                    // next step (.retryWhen(...)) will react to this Flux-error.
                    // Lazily create the handler's context on the first matching signal; reuse the same
                    // instance across all subsequent retries on this Flux.
                    if (triggered.get() == null)
                    {
                        context.set(handler.newContext());
                    }
                    triggered.set(handler);
                    sink.error(new SatelliteSignalException(handler, apiCallRc, context.get()));
                }
            })
            // we want to use a .concatMap instead of .flatMap here to preserve the order of events. flatMap
            // would allow two near-simultaneous signals run their beforeRetrys concurrently, which would
            // produce two re-subscribes.
            .retryWhen(Retry.from(signals -> signals.concatMap(retrySignal ->
            {
                // The function we are currently in ( "rs -> {...}") is subscribed on a reactor-internal
                // RetrySignals-stream. Every flux failure (i.e. the Flux-error from the previous step) will
                // trigger this function. If this function emits a Mono with a value, reactor re-subscribes to
                // previous Flux-step again (after we have modified the state of our data within .beforeRetry(...)).
                // If this function emits an error, reactor treats this as "no more retries, forward the error".

                // Please note that the returned Mono is only supposed to modify the data, NOT start a new
                // updateSatellites on its own. Reactor will re-run the previous peer.apiCall(...) if we return
                // here a the retrySignal.

                Mono<RetrySignal> ret;
                if (retrySignal.failure() instanceof SatelliteSignalException sig)
                {
                    if (retrySignal.totalRetries() < sig.handler.maxRetries())
                    { // retry

                        // we return here a Mono that "handler.beforeRetry" returns. that Mono contains the
                        // retry-handler's logic (repick port, mutate rsc, commit). After that Mono completes
                        // we return the original retrySignal, with which we tell reactor to actually perform
                        // the retry / re-subscribe of the previous Flux-step (the peer.apiCall(...) ), including
                        // its entire .map(..).handle(...).... chain.

                        // As already noted before: DO NOT issue a new updateSatellites within the Mono.
                        // Reactor will re-run the previous peer.apiCall(...)
                        // the next step (.concatWith(Flux.defer(() -> ...)) ) might want to actually perform
                        // another updateSatellites so that *all* satellites are informed about the changed
                        // data. This entire retry-mechanism only communicates with the single satellite
                        // that reported the error.

                        // in case we retry, the entire upstream of retryWhen is re-subscribed, in practice meaning
                        // the singular peer.apiCall(...) toward that one satellite is re-issued.
                        // in other words, .beforeRetry modifies the data and by returning retrySignal we tell
                        // reactor to re-do the last step (updating only the one failed satellite again).
                        // if that succeeds, the new response again goes through the .handle step from earlier
                        // but since the API call succeeded the .handle will return a success/complete signal, i.e.
                        // this .retryWhen is not even triggered again. This completes the original (singular)
                        // updateSatellite.
                        // the next step ( .concatWith(Flux.defer(() -> ...)) ) is needed to again update
                        // *all* satellites about the changes that were made in the meantime.
                        ret = callBeforeRetry(sig, rsc)
                            .thenReturn(retrySignal);
                    }
                    else
                    { // retries exhausted. give up with describing error
                        RetcodeRetryExhaustedException exhaustedExc = exhausted(rsc, sig);
                        errorReporter.reportError(exhaustedExc);
                        ret = Mono.error(exhaustedExc);
                    }
                }
                else
                { // not our (expected) error. propagate as-is
                    ret = Mono.error(retrySignal.failure());
                }
                return ret;
            })))
            .concatWith(Flux.defer(() ->
            {
                // this step needs to run within a Flux.defer simply because otherwise trigger.get() is queried
                // before the Flux was subscribed to. That means that the .handle step had no chance to populate
                // the triggered variable meaning that it would always return null, rendering this step completely
                // useless.
                // if no retry-handler was triggered, there is also no need for a post-retry action -> Flux.empty().
                @Nullable SatelliteRetcodeHandler<?> handler = triggered.get();
                return handler != null ?
                    callAfterSuccess(handler, rsc, context.get()) :
                    Flux.empty();
            }));
    }

    /**
     * Returns {@code true} if {@code throwable} is an in-flight dispatcher signal that is not
     * supposed to escape the dispatcher's own {@code retryWhen} (currently only
     * {@link SatelliteSignalException}). Used by surrounding {@code doOnError} paths to skip
     * error-handling work that would otherwise fire for transient signals.
     *
     * <p>NOTE: {@link RetcodeRetryExhaustedException} is intentionally <em>not</em> matched here.
     * After exhaustion the underlying condition is often transient (e.g. a port hold by another
     * process gets released minutes later), so the resource should still be queued by
     * {@code retryResourceTaskProvider} for a later retry. Each retry round persists newly
     * discovered blocked ports to the node's {@code TcpPortsBlocked} prop, so subsequent
     * retries pick fresh ports and the cluster eventually converges.
     */
    public boolean isDispatcherError(Throwable throwable)
    {
        return throwable instanceof SatelliteSignalException;
    }

    private @Nullable SatelliteRetcodeHandler<?> match(ApiCallRc rc)
    {
        @Nullable SatelliteRetcodeHandler<?> ret = null;
        for (ApiCallRc.RcEntry entry : rc)
        {
            @Nullable SatelliteRetcodeHandler<?> handler = handlersByRetcode.get(entry.getReturnCode());
            if (handler != null)
            {
                ret = handler;
                break;
            }
        }
        return ret;
    }

    // Small generic helpers that "capture" the wildcard CTX so each handler callback can be
    // invoked with its declared context type. The unchecked cast is safe because the context
    // came from this exact handler's newContext() call.
    @SuppressWarnings("unchecked")
    private static <CTX> Mono<Void> callBeforeRetry(SatelliteSignalException stltSigExcRef, Resource rscRef)
    {
        SatelliteRetcodeHandler<CTX> handler = (SatelliteRetcodeHandler<CTX>) stltSigExcRef.handler;
        CTX context = (CTX) stltSigExcRef.context;
        return handler.beforeRetry(rscRef, stltSigExcRef.signalRc, context);
    }

    @SuppressWarnings("unchecked")
    private static <CTX> Flux<ApiCallRc> callAfterSuccess(
        SatelliteRetcodeHandler<CTX> handler,
        Resource rsc,
        @Nullable Object context
    )
    {
        return handler.afterSuccess(rsc, (CTX) context);
    }

    @SuppressWarnings("unchecked")
    private static <CTX> ApiCallRc.RcEntry callDescribeExhaustion(
        SatelliteRetcodeHandler<CTX> handler,
        Resource rsc,
        @Nullable Object context,
        int attempts
    )
    {
        return handler.describeExhaustion(rsc, (CTX) context, attempts);
    }

    private static RetcodeRetryExhaustedException exhausted(Resource rsc, SatelliteSignalException sig)
    {
        // Delegate to the handler so the user-facing message has semantic context (e.g. blocked
        // port numbers, prop names to inspect). The dispatcher itself only knows the retcode as
        // an opaque long, which is not actionable.
        return new RetcodeRetryExhaustedException(
            ApiCallRcImpl.singletonApiCallRc(
                callDescribeExhaustion(sig.handler, rsc, sig.context, sig.handler.maxRetries())
            )
        );
    }

    static final class SatelliteSignalException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        final SatelliteRetcodeHandler<?> handler;
        final ApiCallRc signalRc;
        final @Nullable Object context;

        SatelliteSignalException(
            SatelliteRetcodeHandler<?> handlerRef,
            ApiCallRc signalRcRef,
            @Nullable Object contextRef
        )
        {
            super("Satellite signalled retcode 0x" + Long.toHexString(handlerRef.retcode()));
            handler = handlerRef;
            signalRc = signalRcRef;
            context = contextRef;
        }
    }

    /**
     * Surfaced to the client when a handler's retry budget is exhausted. Carries a clean
     * {@link ApiCallRc} with a user-readable message.
     */
    public static final class RetcodeRetryExhaustedException extends ApiRcException
    {
        private static final long serialVersionUID = 1L;

        RetcodeRetryExhaustedException(ApiCallRc apiCallRcRef)
        {
            super(apiCallRcRef);
        }
    }
}
