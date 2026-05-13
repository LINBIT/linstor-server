package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.logging.StdErrorReporter;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
public class SatelliteRetcodeDispatcherTest
{
    private static final long TEST_RETCODE = 12345L | ApiConsts.MASK_INFO;
    protected static StdErrorReporter errorReporter = new StdErrorReporter(
        "TESTS",
        Paths.get("build/test-logs"),
        true,
        "",
        "INFO",
        "TRACE",
        () -> null
    );

    private Resource rsc;

    @Before
    public void setup() throws Exception
    {
        ResourceDefinition rscDfn = mock(ResourceDefinition.class);
        Node node = mock(Node.class);
        rsc = mock(Resource.class);
        when(rsc.getResourceDefinition()).thenReturn(rscDfn);
        when(rsc.getNode()).thenReturn(node);
        when(rscDfn.getName()).thenReturn(new ResourceName("rscTest"));
        when(node.getName()).thenReturn(new NodeName("nodeTest"));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private SatelliteRetcodeDispatcher createDispatcher(Set<SatelliteRetcodeHandler<?>> handlersRef)
    {
        Object intentionalTypeErasure = handlersRef;
        // the constructor of SatelliteRetcodeDispatcher expects a set of raw SatelliteRetCodeHandlers (no wildcard)
        // since that is also the version it gets injected from Guice. Sorry for this hacky simulation of that, but
        // this way the tests themselves can at least omit suppressing "rawtypes"
        return new SatelliteRetcodeDispatcher(errorReporter, (Set<SatelliteRetcodeHandler>) intentionalTypeErasure);
    }

    @Test
    public void passThroughWhenNoHandlersRegistered()
    {
        SatelliteRetcodeDispatcher dispatcher = new SatelliteRetcodeDispatcher(errorReporter, new HashSet<>());
        ApiCallRc rc = okRc();
        StepVerifier.create(Flux.just(rc).transform(dispatcher.forResource(rsc)))
            .expectNext(rc)
            .verifyComplete();
    }

    @Test
    public void passThroughWhenHandlerDoesNotMatch()
    {
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(TEST_RETCODE, 3, ignored -> { }, Flux::empty));
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);
        ApiCallRc rc = okRc();
        StepVerifier.create(Flux.just(rc).transform(dispatcher.forResource(rsc)))
            .expectNext(rc)
            .verifyComplete();
    }


    @Test
    public void retryUntilSuccessAndAfterSuccessRunsOnce()
    {
        AtomicInteger beforeRetryCalls = new AtomicInteger();
        AtomicInteger afterSuccessCalls = new AtomicInteger();

        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(
            TEST_RETCODE,
            5,
            rc -> beforeRetryCalls.incrementAndGet(),
            () ->
            {
                afterSuccessCalls.incrementAndGet();
                return Flux.empty();
            }
        ));

        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        AtomicInteger attempt = new AtomicInteger();
        Flux<ApiCallRc> source = Flux.defer(() ->
            attempt.incrementAndGet() <= 2 ? Flux.just(signalRc()) : Flux.just(okRc())
        );

        StepVerifier.create(source.transform(dispatcher.forResource(rsc)))
            .expectNextMatches(rc -> rc.size() == 1 && rc.get(0).getReturnCode() == ApiConsts.MODIFIED)
            .verifyComplete();

        assertEquals(2, beforeRetryCalls.get());
        assertEquals(1, afterSuccessCalls.get());
    }

    @Test
    public void afterSuccessNotInvokedIfNoSignalSeen()
    {
        AtomicInteger afterSuccessCalls = new AtomicInteger();
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(
            TEST_RETCODE,
            3,
            ignored -> { },
            () ->
            {
                afterSuccessCalls.incrementAndGet();
                return Flux.empty();
            }
        ));
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        StepVerifier.create(Flux.just(okRc()).transform(dispatcher.forResource(rsc)))
            .expectNextCount(1)
            .verifyComplete();
        assertEquals(0, afterSuccessCalls.get());
    }

    @Test
    public void exhaustionUsesDefaultDescribeExhaustionWhenHandlerDoesNotOverride()
    {
        // The default describeExhaustion uses FAIL_UNKNOWN_ERROR and quotes the last signal
        // message, since the dispatcher itself has no semantic context for the retcode.
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(TEST_RETCODE, 2, ignored -> { }, Flux::empty));
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        Flux<ApiCallRc> source = Flux.defer(() -> Flux.just(signalRc()));

        StepVerifier.create(source.transform(dispatcher.forResource(rsc)))
            .expectErrorMatches(err ->
                // Exhaustion deliberately does NOT count as a dispatcher-internal error so that
                // surrounding doOnError paths still queue the resource for later retry.
                !dispatcher.isDispatcherError(err) &&
                err instanceof SatelliteRetcodeDispatcher.RetcodeRetryExhaustedException exhaustedExc &&
                exhaustedExc.getApiCallRc().get(0).getReturnCode() == ApiConsts.FAIL_UNKNOWN_ERROR &&
                exhaustedExc.getApiCallRc().get(0).getMessage().contains("rscTest")
            )
            .verify();
    }

    @Test
    public void apiRcExceptionWithMatchingRetcodeTriggersRetry()
    {
        // CommonMessageProcessor short-circuits FAIL-mask responses into an ApiRcException
        // before the dispatcher's .handle ever sees them. Verify the dispatcher's
        // onErrorResume unwraps it and routes through the same retry path as a `next`
        // emission.
        AtomicInteger beforeRetryCalls = new AtomicInteger();
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(
            TEST_RETCODE,
            5,
            rc -> beforeRetryCalls.incrementAndGet(),
            Flux::empty
        ));
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        AtomicInteger attempt = new AtomicInteger();
        Flux<ApiCallRc> source = Flux.defer(() ->
            attempt.incrementAndGet() <= 2
                ? Flux.<ApiCallRc>error(new ApiRcException(signalRc()))
                : Flux.just(okRc())
        );

        StepVerifier.create(source.transform(dispatcher.forResource(rsc)))
            .expectNextMatches(rc -> rc.size() == 1 && rc.get(0).getReturnCode() == ApiConsts.MODIFIED)
            .verifyComplete();

        assertEquals(2, beforeRetryCalls.get());
    }

    @Test
    public void apiRcExceptionWithUnmatchedRetcodePropagatesUnchanged()
    {
        // Failures unrelated to any registered handler must keep their identity end-to-end,
        // so downstream error-handling (combineResponses, retryResourceTask) still sees the
        // original ApiRcException.
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(TEST_RETCODE, 3, ignored -> { }, Flux::empty));
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        ApiRcException unrelated = new ApiRcException(ApiCallRcImpl.singletonApiCallRc(
            ApiCallRcImpl.entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR, "boom").build()
        ));

        StepVerifier.create(
            Flux.<ApiCallRc>error(unrelated).transform(dispatcher.forResource(rsc))
        )
            .expectErrorMatches(err -> err == unrelated)
            .verify();
    }

    @Test
    public void exhaustionDelegatesToHandlerDescribeExhaustion()
    {
        // The dispatcher must surface the handler's own describeExhaustion output, not its own
        // generic hex-retcode dump.
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(new SatelliteRetcodeHandler<Void>()
        {
            @Override public long retcode() { return TEST_RETCODE; }
            @Override public int maxRetries() { return 2; }

            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            @Override public Void newContext() { return null; }
            @Override public Mono<Void> beforeRetry(Resource r, ApiCallRc rc, Void c) { return Mono.empty(); }
            @Override public Flux<ApiCallRc> afterSuccess(Resource r, Void c) { return Flux.empty(); }
            @Override
            public ApiCallRc.RcEntry describeExhaustion(Resource r, Void c, int attempts)
            {
                return ApiCallRcImpl.entryBuilder(ApiConsts.FAIL_INVLD_RSC_PORT, "custom message")
                    .setCause("custom cause")
                    .setCorrection("custom correction")
                    .build();
            }
        });
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        StepVerifier.create(
            Flux.defer(() -> Flux.just(signalRc())).transform(dispatcher.forResource(rsc))
        )
            .expectErrorMatches(err ->
                err instanceof SatelliteRetcodeDispatcher.RetcodeRetryExhaustedException exhausted &&
                exhausted.getApiCallRc().get(0).getReturnCode() == ApiConsts.FAIL_INVLD_RSC_PORT &&
                "custom message".equals(exhausted.getApiCallRc().get(0).getMessage()) &&
                "custom cause".equals(exhausted.getApiCallRc().get(0).getCause()) &&
                "custom correction".equals(exhausted.getApiCallRc().get(0).getCorrection())
            )
            .verify();
    }

    @Test
    public void newContextCalledOncePerFluxAndSharedAcrossCallbacks()
    {
        // Pins the lifecycle invariant: newContext() is invoked exactly once on the first
        // matching signal, and the same instance is threaded through every subsequent
        // beforeRetry call and the final afterSuccess.
        AtomicInteger newContextCalls = new AtomicInteger();
        java.util.List<Object> contextsSeen = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(new SatelliteRetcodeHandler<>()
        {
            @Override public long retcode() { return TEST_RETCODE; }
            @Override public int maxRetries() { return 5; }
            @Override public Object newContext()
            {
                newContextCalls.incrementAndGet();
                return new Object();
            }
            @Override public Mono<Void> beforeRetry(Resource r, ApiCallRc rc, Object ctx)
            {
                contextsSeen.add(ctx);
                return Mono.empty();
            }
            @Override public Flux<ApiCallRc> afterSuccess(Resource r, Object ctx)
            {
                contextsSeen.add(ctx);
                return Flux.empty();
            }
        });
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        AtomicInteger attempt = new AtomicInteger();
        Flux<ApiCallRc> source = Flux.defer(() ->
            attempt.incrementAndGet() <= 3 ? Flux.just(signalRc()) : Flux.just(okRc())
        );

        StepVerifier.create(source.transform(dispatcher.forResource(rsc)))
            .expectNextCount(1)
            .verifyComplete();

        assertEquals(1, newContextCalls.get());
        assertEquals(4, contextsSeen.size()); // 3 beforeRetry + 1 afterSuccess
        Object first = contextsSeen.get(0);
        assertTrue("first context non-null", first != null);
        for (Object seen : contextsSeen)
        {
            assertTrue("every callback got the same context instance", seen == first);
        }
    }

    @Test
    public void isDispatcherErrorOnlyMatchesInflightSignalNotExhaustion()
    {
        // Pins the doOnError-in-CtrlSatelliteUpdateCaller contract: SatelliteSignalException is
        // in-flight and should be filtered out (it never escapes), but RetcodeRetryExhaustedException
        // must NOT be filtered out so the resource still ends up on retryResourceTaskProvider.
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(TEST_RETCODE, 1, ignored -> { }, Flux::empty));
        SatelliteRetcodeDispatcher dispatcher = createDispatcher(handlers);

        SatelliteRetcodeHandler<?> anyHandler = handlers.iterator().next();
        Throwable inflight = new SatelliteRetcodeDispatcher.SatelliteSignalException(
            anyHandler, signalRc(), null
        );
        Throwable exhausted = new SatelliteRetcodeDispatcher.RetcodeRetryExhaustedException(okRc());

        assertTrue("SatelliteSignalException is dispatcher-internal", dispatcher.isDispatcherError(inflight));
        org.junit.Assert.assertFalse(
            "RetcodeRetryExhaustedException must escape to surrounding doOnError",
            dispatcher.isDispatcherError(exhausted)
        );
    }

    @Test
    public void duplicateRetcodeRegistrationThrows()
    {
        Set<SatelliteRetcodeHandler<?>> handlers = new HashSet<>();
        handlers.add(stubHandler(TEST_RETCODE, 1, ignored -> { }, Flux::empty));
        handlers.add(stubHandler(TEST_RETCODE, 1, ignored -> { }, Flux::empty));
        try
        {
            createDispatcher(handlers);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected)
        {
            assertTrue(expected.getMessage().contains("0x" + Long.toHexString(TEST_RETCODE)));
        }
    }

    private static ApiCallRc okRc()
    {
        return ApiCallRcImpl.singletonApiCallRc(
            ApiCallRcImpl.entryBuilder(ApiConsts.MODIFIED, "ok").build()
        );
    }

    private static ApiCallRc signalRc()
    {
        return ApiCallRcImpl.singletonApiCallRc(
            ApiCallRcImpl.entryBuilder(TEST_RETCODE, "signal").build()
        );
    }

    private static SatelliteRetcodeHandler<Void> stubHandler(
        long retcodeRef,
        int maxRetriesRef,
        Consumer<ApiCallRc> onBeforeRetryRef,
        Supplier<Flux<ApiCallRc>> afterSuccessSupplierRef
    )
    {
        return new SatelliteRetcodeHandler<>()
        {
            @Override
            public long retcode()
            {
                return retcodeRef;
            }

            @Override
            public int maxRetries()
            {
                return maxRetriesRef;
            }

            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            @Override
            public Void newContext()
            {
                return null;
            }

            @Override
            public Mono<Void> beforeRetry(Resource rscArg, ApiCallRc signalRcArg, Void ctx)
            {
                onBeforeRetryRef.accept(signalRcArg);
                return Mono.empty();
            }

            @Override
            public Flux<ApiCallRc> afterSuccess(Resource rscArg, Void ctx)
            {
                return afterSuccessSupplierRef.get();
            }
        };
    }
}
