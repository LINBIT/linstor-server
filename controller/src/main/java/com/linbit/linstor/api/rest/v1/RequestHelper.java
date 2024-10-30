package com.linbit.linstor.api.rest.v1;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.annotation.ErrorReporterContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.CtrlAuthentication;
import com.linbit.linstor.security.IdentityName;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SignInException;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Key;
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
    private final AccessContext sysContext;
    private final AccessContext publicContext;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final CtrlAuthentication<ControllerDatabase> authentication;
    private final CtrlConfig linstorConfig;

    @Inject
    public RequestHelper(
        ErrorReporter errorReporterRef,
        LinStorScope apiCallScopeRef,
        @SystemContext AccessContext sysContextRef,
        @PublicContext AccessContext accessContextRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        CtrlAuthentication<ControllerDatabase> authenticationRef,
        CtrlConfig linstorConfigRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallScope = apiCallScopeRef;
        sysContext = sysContextRef;
        publicContext = accessContextRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        authentication = authenticationRef;
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

    private void checkLDAPAuth(Peer peer, String authHeader)
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
                    AccessContext clientCtx = authentication.signIn(
                        new IdentityName(user),
                        password.getBytes(StandardCharsets.UTF_8)
                    );
                    AccessContext privCtx = sysContext.clone();
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                    peer.setAccessContext(privCtx, clientCtx);
                }
                catch (AccessDeniedException accExc)
                {
                    throw new ImplementationError(
                        "Enabling privileges on the system context failed",
                        accExc
                    );
                }
                catch (InvalidNameException nameExc)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.singleApiCallRc(ApiConsts.FAIL_SIGN_IN, nameExc.getMessage())
                    );
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
        Peer peer = new PeerREST(request.getRemoteAddr(), userAgent, publicContext);

        checkLDAPAuth(peer, request.getAuthorization());

        errorReporter.logInfo("REST/API %s/%s", peer.toString(), apiCall);
        return Context.of(
            ApiModule.API_CALL_NAME, apiCall,
            AccessContext.class, peer.getAccessContext(),
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
        AccessContext accCtx = subscriberContext.get(AccessContext.class);
        Peer peer = subscriberContext.getOrDefault(Peer.class, null);

        Response ret;

        TransactionMgr transMgr = transactional ? transactionMgrGenerator.startTransaction() : null;

        try (LinStorScope.ScopeAutoCloseable close = apiCallScope.enter())
        {
            apiCallScope.seed(Key.get(AccessContext.class, PeerContext.class), accCtx);
            apiCallScope.seed(Key.get(AccessContext.class, ErrorReporterContext.class), accCtx);
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
                .entity(ApiCallRcRestUtils.toJSON(apiCallRc))
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
                .entity(ApiCallRcRestUtils.toJSON(apiCallRc))
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
                            accCtx,
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
            .entity(ApiCallRcRestUtils.toJSON(apiCallRc))
            .type(MediaType.APPLICATION_JSON)
            .build();
    }


    static Response queryRequestResponse(
        ObjectMapper objectMapper,
        long retCode,
        String objectType,
        String searchObject,
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
