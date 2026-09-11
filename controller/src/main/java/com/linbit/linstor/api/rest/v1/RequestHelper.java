package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
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
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Histogram;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import org.slf4j.event.Level;
import reactor.core.publisher.Mono;
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

    @Inject
    public RequestHelper(
        ErrorReporter errorReporterRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        LdapAuthentication ldapAuthenticationRef,
        CtrlConfig linstorConfigRef
    )
    {
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
            .subscribe(
                asyncResponse::resume,
                exc ->
                {
                    String errId = errorReporter.reportError(exc);
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, exc.getMessage())
                            .addErrorId(errId));
                    asyncResponse.resume(
                        ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.INTERNAL_SERVER_ERROR)
                    );
                }
            );
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
