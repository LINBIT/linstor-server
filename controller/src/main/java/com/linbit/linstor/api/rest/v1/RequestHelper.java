package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.rest.v1.config.ResponseBudget;
import com.linbit.linstor.api.rest.v1.config.ResponseBudgetFilter;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerREST;
import com.linbit.linstor.prometheus.LinstorControllerMetrics;
import com.linbit.linstor.security.LdapAuthentication;
import com.linbit.linstor.security.SignInException;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Histogram;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class RequestHelper
{
    protected final ErrorReporter errorReporter;
    private final LinStorScope apiCallScope;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final LdapAuthentication ldapAuthentication;
    private final CtrlConfig linstorConfig;
    private final Scheduler restResponseScheduler;

    private final ResponseBudget responseBudget;

    /** Causes followed when looking for a RejectedExecutionException. */
    private static final int MAX_CAUSE_DEPTH = 8;
    /** Seconds a client is asked to wait when the RestResponse queue is full (503). */
    static final String RETRY_AFTER_SEC = "5";

    @Inject
    public RequestHelper(
        ErrorReporter errorReporterRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        LdapAuthentication ldapAuthenticationRef,
        CtrlConfig linstorConfigRef,
        @Named(ControllerCoreModule.REST_RESPONSE_SCHEDULER) Scheduler restResponseSchedulerRef
    )
    {
        this(
            errorReporterRef,
            apiCallScopeRef,
            transactionMgrGeneratorRef,
            ldapAuthenticationRef,
            linstorConfigRef,
            restResponseSchedulerRef,
            ResponseBudget.shared()
        );
    }

    // package-private for tests: a budget of their own instead of the controller-wide shared one
    RequestHelper(
        ErrorReporter errorReporterRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        LdapAuthentication ldapAuthenticationRef,
        CtrlConfig linstorConfigRef,
        Scheduler restResponseSchedulerRef,
        ResponseBudget responseBudgetRef
    )
    {
        responseBudget = responseBudgetRef;
        restResponseScheduler = restResponseSchedulerRef;
        errorReporter = errorReporterRef;
        apiCallScope = apiCallScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        ldapAuthentication = ldapAuthenticationRef;
        linstorConfig = linstorConfigRef;
    }

    private Tuple2<String, String> parseBasicAuthHeader(String authorization)
    {
        String user = "";
        String password = "";
        String[] authFields = authorization.split(" ", 2);
        if (authFields.length > 0)
        {
            if (authFields[0].equals("Basic"))
            {
                if (authFields.length > 1)
                {
                    String authToken = new String(
                        Base64.getDecoder().decode(authFields[1]),
                        StandardCharsets.UTF_8
                    );

                    final String[] authTokenFields = authToken.split(":", 2);
                    if (authTokenFields.length > 1)
                    {
                        user = authTokenFields[0];
                        password = authTokenFields[1];
                    }
                }
                else
                {
                    ApiCallRcImpl apiCallRc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_INVLD_ENCRYPT_TYPE,
                        "Basic authentication doesn't contain credential token."
                    );
                    throw new ApiRcException(apiCallRc);
                }
            }
            else
            {
                ApiCallRcImpl apiCallRc = ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_INVLD_ENCRYPT_TYPE,
                    "Invalid Authorization method, only 'Basic' supported."
                );
                throw new ApiRcException(apiCallRc);
            }
        }
        return Tuples.of(user, password);
    }

    private void checkLDAPAuth(String authHeader)
    {
        if (linstorConfig.isLdapEnabled())
        {
            // request.getAuthorization() contains authorization http field
            if (authHeader != null)
            {
                Tuple2<String, String> userPassword = parseBasicAuthHeader(authHeader);
                String user = userPassword.getT1();
                String password = userPassword.getT2();

                try
                {
                    ldapAuthentication.authenticate(user, password.getBytes(StandardCharsets.UTF_8));
                }
                catch (SignInException signIgnExc)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.singleApiCallRc(ApiConsts.FAIL_SIGN_IN, signIgnExc)
                    );
                }
            }
            else
            {
                if (!linstorConfig.isLdapPublicAccessAllowed())
                {
                    ApiCallRc apiCallRc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_SIGN_IN_MISSING_CREDENTIALS,
                        "Login required but no 'Authorization' header given."
                    );
                    throw new ApiRcException(apiCallRc);
                }
            }
        }
    }

    public Context createContext(String apiCall, Request request)
    {
        if (MDC.get(ErrorReporter.LOGID) == null)
        {
            MDC.put(ErrorReporter.LOGID, ErrorReporter.getNewLogId());
        }
        final String userAgent = request.getHeader("User-Agent");
        PeerREST peer = new PeerREST(request.getRemoteAddr(), userAgent);

        checkLDAPAuth(request.getAuthorization());

        errorReporter.logInfo("REST/API %s/%s", peer.toString(), apiCall);
        return Context.of(
            ApiModule.API_CALL_NAME, apiCall,
            Peer.class, peer,
            ErrorReporter.LOGID, MDC.get(ErrorReporter.LOGID)
        );
    }

    public Response doInScope(
        String apiCall,
        Request request,
        Callable<Response> callable,
        boolean transactional
    )
    {
        Context subscriberContext = createContext(apiCall, request);
        Peer peer = subscriberContext.getOrDefault(Peer.class, null);

        Response ret;

        TransactionMgr transMgr = transactional ? transactionMgrGenerator.startTransaction() : null;

        try (LinStorScope.ScopeAutoCloseable close = apiCallScope.enter())
        {
            apiCallScope.seed(Peer.class, peer);

            if (transMgr != null)
            {
                TransactionMgrUtil.seedTransactionMgr(apiCallScope, transMgr);
            }

            try (Histogram.Timer ignored = LinstorControllerMetrics.requestDurationHistogram.labels(apiCall).startTimer())
            {
                ret = callable.call();
            }
        }
        catch (JsonMappingException | JsonParseException exc)
        {
            String errorReport = errorReporter.reportError(exc);
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(ApiConsts.API_CALL_PARSE_ERROR, "Unable to parse input json.")
                    .setDetails(exc.getMessage())
                    .addErrorId(errorReport)
                    .build()
            );
            ret = Response
                .status(Response.Status.BAD_REQUEST)
                .type(MediaType.APPLICATION_JSON)
                .entity(ApiCallRcRestUtils.toJSON(errorReporter, apiCallRc))
                .build();
        }
        catch (ApiRcException exc)
        {
            errorReporter.logError(exc.getMessage());
            ret = ApiCallRcRestUtils.toResponse(exc.getApiCallRc(), Response.Status.INTERNAL_SERVER_ERROR);
        }
        catch (Throwable exc)
        {
            String errorReport = errorReporter.reportError(exc);
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR, "Exception thrown.")
                    .setDetails(exc.getMessage())
                    .addErrorId(errorReport)
                    .build()
            );
            ret = Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .type(MediaType.APPLICATION_JSON)
                .entity(ApiCallRcRestUtils.toJSON(errorReporter, apiCallRc))
                .build();
        }
        finally
        {
            if (transMgr != null)
            {
                if (transMgr.isDirty())
                {
                    try
                    {
                        transMgr.rollback();
                    }
                    catch (TransactionException sqlExc)
                    {
                        errorReporter.reportError(
                            Level.ERROR,
                            sqlExc,
                            peer,
                            "A database error occurred while trying to rollback"
                        );
                    }
                }
                transMgr.returnConnection();
            }
        }

        return ret;
    }

    void doFlux(
        String apiCall,
        Request request,
        final AsyncResponse asyncResponse,
        Mono<Response> monoResponse
    )
    {
        Context context = createContext(apiCall, request);

        Mono.using(
                () -> LinstorControllerMetrics.requestDurationHistogram.labels(apiCall).startTimer(),
                (ignored) -> monoResponse,
                Histogram.Timer::close
            )
            .contextWrite(context)
            .onErrorResume(
                ApiRcException.class,
                apiExc -> Mono.just(
                    ApiCallRcRestUtils.toResponse(apiExc.getApiCallRc(), Response.Status.INTERNAL_SERVER_ERROR)))
            .onErrorResume(
                CtrlResponseUtils.DelayedApiRcException.class,
                delExc -> {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    for (var exc : delExc.getErrors())
                    {
                        if (exc.getApiCallRc().allSkipErrorReport())
                        {
                            errorReporter.logError(exc.getMessage());
                            apiCallRc.addEntry(
                                ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, exc.getMessage()));
                        }
                        else
                        {
                            String errId = errorReporter.reportError(exc);
                            apiCallRc.addEntry(
                                ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, exc.getMessage())
                                    .addErrorId(errId));
                        }
                    }
                    return Mono.just(ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.INTERNAL_SERVER_ERROR));
                })
            // never write the HTTP response on the thread that completed the satellite call (the in-order
            // satellite message worker): a slow or gone client would hold that satellite's turn, and with it the whole
            // MainWorkerPool, until Grizzly's write timeout.
            .publishOn(restResponseScheduler)
            .subscribe(
                asyncResponse::resume,
                exc -> resumeWithError(asyncResponse, exc)
            );
    }

    /**
     * The RestResponse scheduler for REST endpoints that subscribe to their response themselves.
     */
    Scheduler getRestResponseScheduler()
    {
        return restResponseScheduler;
    }

    /**
     * Hop a satellite-fed flux onto the RestResponse scheduler before the endpoint maps it into a large
     * JSON body. Without this, the render (e.g. objectMapper.writeValueAsString of the 14-MB resource view) runs on the
     * in-order satellite message worker that completed the last fetch, upstream of doFlux's publishOn.
     */
    <T> Flux<T> offWorker(Flux<T> flux)
    {
        return flux.publishOn(restResponseScheduler);
    }

    /**
     * Like {@link #offWorker(Flux)}, and skip the render if the client's connection is already closed
     * when the data arrives. The answer then ends as {@link ClientGoneException}, which
     * {@link #resumeWithError(AsyncResponse, Throwable)} drops without rendering or reporting.
     */
    <T> Flux<T> offWorker(Flux<T> flux, Request request)
    {
        return Flux.defer(() ->
        {
            // Admission at the entry of the list endpoint, before subscribing to the satellite-fed flux. That flux is
            // lazy (ScopeRunner defers), so a refused request does no satellite fan-out and no walk of the objects
            // under the read locks. The ticket lives in this closure until the flux terminates (after the render,
            // which runs synchronously downstream in onNext), on error and on cancel.
            AtomicReference<ResponseBudget.@Nullable RenderTicket> ticketRef = new AtomicReference<>();
            Flux<T> ret;
            if (!isClientConnected(request))
            {
                ret = Flux.error(new ClientGoneException());
            }
            else if (!admitRender(request, ticketRef))
            {
                // make a pinned client visible
                errorReporter.logWarning(
                    "REST answer refused before render: %s",
                    responseBudget.describeClient(ResponseBudgetFilter.clientKeyOf(request))
                );
                ret = Flux.error(new RenderRefusedException());
            }
            else
            {
                ret = offWorker(flux)
                    .<T>handle(
                        (item, sink) ->
                        {
                            // the client may have gone during the fan-out and the walk
                            if (isClientConnected(request))
                            {
                                sink.next(item);
                            }
                            else
                            {
                                sink.error(new ClientGoneException());
                            }
                        }
                    )
                    .doFinally(
                        ignored ->
                        {
                            ResponseBudget.@Nullable RenderTicket ticket = ticketRef.getAndSet(null);
                            if (ticket != null)
                            {
                                ticket.close();
                            }
                        }
                    );
            }
            return ret;
        });
    }

    private boolean admitRender(Request request, AtomicReference<ResponseBudget.@Nullable RenderTicket> ticketRef)
    {
        ResponseBudget.@Nullable RenderTicket ticket = responseBudget.tryAdmitRender(
            ResponseBudgetFilter.clientKeyOf(request),
            ResponseBudgetFilter.requestTarget(request)
        );
        ticketRef.set(ticket);
        if (ticket != null)
        {
            // hand the ticket (and a reservation, if any) to ResponseBudgetFilter, which turns it into the lease
            request.setAttribute(ResponseBudget.RENDER_TICKET_ATTR, ticket);
        }
        return ticket != null;
    }

    /** The client already has its share of large answers or renders in flight. */
    static final class RenderRefusedException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        RenderRefusedException()
        {
            super(
                "REST response budget: client has its share of large answers or renders in flight",
                null,
                false,
                false
            );
        }
    }

    static boolean isClientConnected(@Nullable Request request)
    {
        boolean connected = true;
        try
        {
            if (request != null && request.getContext() != null && request.getContext().getConnection() != null)
            {
                connected = request.getContext().getConnection().isOpen();
            }
        }
        catch (RuntimeException ignored)
        {
            // cannot tell: render as before
        }
        return connected;
    }

    /** The client closed its connection before its answer was rendered. */
    static final class ClientGoneException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        ClientGoneException()
        {
            super("REST client closed the connection before the answer was rendered", null, false, false);
        }
    }

    /**
     * Error path of a REST response. A rejected publishOn (RestResponse queue full) becomes 503 + Retry-After without
     * an error report; everything else is reported and answered with 500, as before.
     * Runs on the calling thread; both answers are small and do not block on a slow client.
     */
    void resumeWithError(AsyncResponse asyncResponse, Throwable exc)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        if (exc instanceof ClientGoneException)
        {
            // nobody is left to read an answer; end the request without a body
            errorReporter.logDebug("REST answer dropped: %s", exc.getMessage());
            asyncResponse.cancel();
        }
        else if (exc instanceof RenderRefusedException)
        {
            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Controller REST response budget exhausted, retry in " + RETRY_AFTER_SEC + " s"
                )
            );
            asyncResponse.resume(serviceUnavailable(apiCallRc));
        }
        else if (isRejected(exc))
        {
            errorReporter.logWarning("REST response queue full, answering 503: %s", exc.getMessage());
            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Controller busy writing REST responses, retry in " + RETRY_AFTER_SEC + " s"
                )
            );
            asyncResponse.resume(serviceUnavailable(apiCallRc));
        }
        else
        {
            String errId = errorReporter.reportError(exc);
            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, exc.getMessage())
                    .addErrorId(errId));
            asyncResponse.resume(
                ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.INTERNAL_SERVER_ERROR)
            );
        }
    }

    /**
     * 503 + Retry-After with the usual ApiCallRc body. ApiCallRcRestUtils.toResponse(rc, status) uses the given status
     * only for a success; with an error entry it answers 500. So the status is set here explicitly.
     */
    static Response serviceUnavailable(ApiCallRcImpl apiCallRc)
    {
        Response errorBody = ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.SERVICE_UNAVAILABLE);
        return Response.fromResponse(errorBody)
            .status(Response.Status.SERVICE_UNAVAILABLE)
            .header("Retry-After", RETRY_AFTER_SEC)
            .build();
    }

    static boolean isRejected(@Nullable Throwable exc)
    {
        boolean rejected = false;
        Throwable cur = exc;
        int depth = 0;
        while (!rejected && cur != null && depth < MAX_CAUSE_DEPTH)
        {
            rejected = cur instanceof RejectedExecutionException;
            cur = cur.getCause();
            depth++;
        }
        return rejected;
    }

    static void safeAsyncResponse(AsyncResponse asyncResponse, Runnable restAction)
    {
        try
        {
            restAction.run();
        }
        catch (ApiRcException apiExc)
        {
            asyncResponse.resume(
                ApiCallRcRestUtils.toResponse(apiExc.getApiCallRc(), Response.Status.INTERNAL_SERVER_ERROR)
            );
        }
    }

    /**
     * Parses {@code jsonData} into the given type, falling back to {@code defaultSupplier} when
     * the body is missing, empty, or the JSON literal {@code null} (which Jackson deserializes as
     * a {@code null} object reference and would NPE on subsequent field access).
     */
    public static <T> T parseJsonOrDefault(
        ObjectMapper objectMapper,
        @Nullable String jsonData,
        Class<T> type,
        Supplier<T> defaultSupplier
    )
        throws JsonProcessingException
    {
        if (jsonData != null && !jsonData.trim().isEmpty())
        {
            T parsed = objectMapper.readValue(jsonData, type);
            // Jackson returns null for the JSON 'null' literal; static analysis misses this case.
            //noinspection ConstantValue
            if (parsed != null)
            {
                return parsed;
            }
        }
        return defaultSupplier.get();
    }

    static Response notFoundResponse(final long retcode, final String message)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                retcode,
                message
            )
        );
        return Response
            .status(Response.Status.NOT_FOUND)
            .entity(ApiCallRcRestUtils.toJSONCatch(apiCallRc))
            .type(MediaType.APPLICATION_JSON)
            .build();
    }


    static Response queryRequestResponse(
        ObjectMapper objectMapper,
        long retCode,
        String objectType,
        @Nullable String searchObject,
        List<?> resultList
    )
        throws JsonProcessingException
    {
        Response response;
        if (searchObject != null && resultList.isEmpty())
        {
            response = RequestHelper.notFoundResponse(
                retCode, String.format("%s '%s' not found.", objectType, searchObject)
            );
        }
        else
        {
            response = Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(searchObject != null ? resultList.get(0) : resultList))
                .build();
        }
        return response;
    }
}
