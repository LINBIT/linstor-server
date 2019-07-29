package com.linbit.linstor.api.rest.v1;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.LinstorConfigToml;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerREST;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.CtrlAuthentication;
import com.linbit.linstor.security.IdentityName;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SignInException;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrGenerator;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Key;
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
    private final CtrlAuthentication authentication;
    private final LinstorConfigToml linstorConfig;

    @Inject
    public RequestHelper(
            ErrorReporter errorReporterRef,
            LinStorScope apiCallScopeRef,
            @SystemContext AccessContext sysContextRef,
            @PublicContext AccessContext accessContextRef,
            TransactionMgrGenerator transactionMgrGeneratorRef,
            CtrlAuthentication authenticationRef,
            LinstorConfigToml linstorConfigRef
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
        if (linstorConfig.getLDAP().isEnabled())
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
                if (!linstorConfig.getLDAP().allowPublicAccess())
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

    Context createContext(String apiCall, org.glassfish.grizzly.http.server.Request request)
    {
        final String userAgent = request.getHeader("User-Agent");
        Peer peer = new PeerREST(request.getRemoteAddr(), userAgent, publicContext);

        checkLDAPAuth(peer, request.getAuthorization());

        errorReporter.logDebug("REST access api '%s' from '%s'", apiCall, peer.toString());
        return  Context.of(
            ApiModule.API_CALL_NAME, apiCall,
            AccessContext.class, peer.getAccessContext(),
            Peer.class, peer
        );
    }

    Response doInScope(
        String apiCall,
        org.glassfish.grizzly.http.server.Request request,
        Callable<Response> callable,
        boolean transactional
    )
    {
        return doInScope(createContext(apiCall, request), callable, transactional);
    }

    Response doInScope(
        Context subscriberContext,
        Callable<Response> callable,
        boolean transactional
    )
    {
        AccessContext accCtx = subscriberContext.get(AccessContext.class);
        Peer peer = subscriberContext.getOrDefault(Peer.class, null);

        Response ret;

        TransactionMgr transMgr = transactional ? transactionMgrGenerator.startTransaction() : null;

        apiCallScope.enter();
        try
        {
            apiCallScope.seed(Key.get(AccessContext.class, PeerContext.class), accCtx);
            apiCallScope.seed(Peer.class, peer);

            if (transMgr != null)
            {
                apiCallScope.seed(TransactionMgr.class, transMgr);
            }

            ret = callable.call();
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
                .entity(ApiCallRcConverter.toJSON(apiCallRc))
                .build();
        }
        catch (ApiRcException exc)
        {
            ret = ApiCallRcConverter.toResponse(exc.getApiCallRc(), Response.Status.INTERNAL_SERVER_ERROR);
        }
        catch (Exception exc)
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
                .entity(ApiCallRcConverter.toJSON(apiCallRc))
                .build();
        }
        finally
        {
            apiCallScope.exit();
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

    void doFlux(final AsyncResponse asyncResponse, Mono<Response> monoResponse)
    {
        monoResponse
            .onErrorResume(ApiRcException.class,
                apiExc -> Mono.just(
                    ApiCallRcConverter.toResponse(apiExc.getApiCallRc(), Response.Status.INTERNAL_SERVER_ERROR)))
            .subscribe(
                asyncResponse::resume,
                exc ->
                {
                    errorReporter.reportError(exc);
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, exc.getMessage()));
                    asyncResponse.resume(
                        ApiCallRcConverter.toResponse(apiCallRc, Response.Status.INTERNAL_SERVER_ERROR)
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
                ApiCallRcConverter.toResponse(apiExc.getApiCallRc(), Response.Status.INTERNAL_SERVER_ERROR)
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
            .entity(ApiCallRcConverter.toJSON(apiCallRc))
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
