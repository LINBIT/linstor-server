package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.rest.v1.config.ResponseBudget;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.security.LdapAuthentication;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.glassfish.grizzly.http.server.Request;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code offWorker(flux, request)} admits the render against the shared budget once the data arrives; past the
 * client's renders cap the answer is refused at once; the ticket returns when the flux terminates or is cancelled.
 */
public class RequestHelperAdmitRenderTest
{
    private static final long MIB = 1024L * 1024L;
    private final ResponseBudget budget = new ResponseBudget(1000 * MIB, 64 * MIB, 4, 8);
    private RequestHelper helper;
    private String client;

    @Before
    public void setUp()
    {
        helper = new RequestHelper(
            new EmptyErrorReporter(),
            mock(LinStorScope.class),
            mock(TransactionMgrGenerator.class),
            mock(LdapAuthentication.class),
            mock(CtrlConfig.class),
            Schedulers.immediate(),
            budget
        );
        client = "test-" + UUID.randomUUID();
    }

    private Request request()
    {
        Request req = mock(Request.class);
        when(req.getRemoteAddr()).thenReturn(client);
        when(req.getRequestURI()).thenReturn("/v1/view/resources");
        return req;
    }

    @Test
    public void rendersBeyondTheCapAreRefusedAndTicketsReturnOnTermination()
    {
        int cap = budget.getPerClientCapRenders();
        List<Disposable> inFlight = new ArrayList<>();
        try
        {
            // renders "in flight": the item arrived and was admitted, the flux has not terminated yet
            for (int idx = 0; idx < cap; idx++)
            {
                inFlight.add(helper.offWorker(Flux.just("data").concatWith(Flux.never()), request()).subscribe());
            }

            AtomicReference<Throwable> refused = new AtomicReference<>();
            helper.offWorker(Flux.just("data"), request()).subscribe(ignored -> { }, refused::set);
            assertTrue(
                "expected a refusal, got " + refused.get(),
                refused.get() instanceof RequestHelper.RenderRefusedException
            );

            // cancel one render: its ticket returns, and a new one is admitted (and returns on completion)
            inFlight.remove(0).dispose();
            assertEquals(List.of("data"), helper.offWorker(Flux.just("data"), request()).collectList().block());
            assertEquals(List.of("data"), helper.offWorker(Flux.just("data"), request()).collectList().block());
        }
        finally
        {
            inFlight.forEach(Disposable::dispose);
        }
    }

    @Test
    public void admittedTicketIsHandedToTheFilterOnTheRequest()
    {
        Request req = request();
        helper.offWorker(Flux.just("data"), req).collectList().block();
        verify(req).setAttribute(eq(ResponseBudget.RENDER_TICKET_ATTR), org.mockito.ArgumentMatchers.notNull());
        assertNotNull(budget);
    }

    /** Counts subscriptions = satellite fan-outs + object walks that actually start; never completes. */
    private static Flux<String> walk(java.util.concurrent.atomic.AtomicInteger walks)
    {
        return Flux.defer(() ->
        {
            walks.incrementAndGet();
            return Flux.<String>never();
        });
    }

    @Test
    public void parallelRequestsForAKnownLargeTargetStartOnlyFourWalks()
    {
        // 8 parallel from one address for a target known to be ~14 MB -> 4 walks, 4 refused without a walk
        budget.recordAnswerSize(client, "/v1/view/resources", 14 * MIB);
        java.util.concurrent.atomic.AtomicInteger walks = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger refused = new java.util.concurrent.atomic.AtomicInteger();
        List<Disposable> inFlight = new ArrayList<>();
        try
        {
            for (int idx = 0; idx < 8; idx++)
            {
                inFlight.add(
                    helper.offWorker(walk(walks), request()).subscribe(
                        ignored -> { },
                        exc ->
                        {
                            if (exc instanceof RequestHelper.RenderRefusedException)
                            {
                                refused.incrementAndGet();
                            }
                        }
                    )
                );
            }
            assertEquals(4, walks.get());
            assertEquals(4, refused.get());
        }
        finally
        {
            inFlight.forEach(Disposable::dispose);
        }
        assertEquals(0, budget.getGlobalUsedBytes()); // the reservations are back
    }

    @Test
    public void firstBurstForANeverSeenTargetIsCappedByTheRendersCap()
    {
        // documented limit: without a recorded size, up to the renders cap (8) walks start per address
        java.util.concurrent.atomic.AtomicInteger walks = new java.util.concurrent.atomic.AtomicInteger();
        List<Disposable> inFlight = new ArrayList<>();
        try
        {
            for (int idx = 0; idx < 10; idx++)
            {
                inFlight.add(helper.offWorker(walk(walks), request()).subscribe(ignored -> { }, ignored -> { }));
            }
            assertEquals(budget.getPerClientCapRenders(), walks.get());
        }
        finally
        {
            inFlight.forEach(Disposable::dispose);
        }
    }

    @Test
    public void earlyRefusalThroughDoFluxIs503WithRetryAfter()
    {
        // the refusal before the fan-out must reach the client as 503 + Retry-After, not as 500
        budget.recordAnswerSize(client, "/v1/view/resources", 14 * MIB);
        List<Disposable> inFlight = new ArrayList<>();
        try
        {
            for (int idx = 0; idx < 4; idx++)
            {
                java.util.concurrent.atomic.AtomicInteger walks = new java.util.concurrent.atomic.AtomicInteger();
                inFlight.add(helper.offWorker(walk(walks), request()).subscribe());
            }
            jakarta.ws.rs.container.AsyncResponse asyncResponse = mock(jakarta.ws.rs.container.AsyncResponse.class);
            Request req = request();
            helper.doFlux(
                "LstVlm",
                req,
                asyncResponse,
                helper.offWorker(Flux.just("data"), req)
                    .map(ignored -> jakarta.ws.rs.core.Response.ok().build())
                    .next()
            );
            org.mockito.ArgumentCaptor<Object> resumed = org.mockito.ArgumentCaptor.forClass(Object.class);
            org.mockito.Mockito.verify(asyncResponse, org.mockito.Mockito.timeout(5_000)).resume(resumed.capture());
            Object value = resumed.getValue();
            assertTrue("expected a Response, got " + value, value instanceof jakarta.ws.rs.core.Response);
            jakarta.ws.rs.core.Response response = (jakarta.ws.rs.core.Response) value;
            assertEquals(503, response.getStatus());
            assertEquals("5", response.getHeaderString("Retry-After"));
        }
        finally
        {
            inFlight.forEach(Disposable::dispose);
        }
    }

    @Test
    public void fullResponseQueueIs503WithRetryAfter()
    {
        // the response-queue-full path, same toResponse problem: a rejected publishOn must answer 503, not 500
        jakarta.ws.rs.container.AsyncResponse asyncResponse = mock(jakarta.ws.rs.container.AsyncResponse.class);
        helper.resumeWithError(asyncResponse, new java.util.concurrent.RejectedExecutionException("queue full"));
        org.mockito.ArgumentCaptor<Object> resumed = org.mockito.ArgumentCaptor.forClass(Object.class);
        org.mockito.Mockito.verify(asyncResponse).resume(resumed.capture());
        jakarta.ws.rs.core.Response response = (jakarta.ws.rs.core.Response) resumed.getValue();
        assertEquals(503, response.getStatus());
        assertEquals("5", response.getHeaderString("Retry-After"));
    }
}
