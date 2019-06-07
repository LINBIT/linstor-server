package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerREST;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrGenerator;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.linbit.linstor.annotation.PublicContext;
import org.slf4j.event.Level;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class RequestHelper
{
    protected final ErrorReporter errorReporter;
    private final LinStorScope apiCallScope;
    private final AccessContext publicContext;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public RequestHelper(
            ErrorReporter errorReporterRef,
            LinStorScope apiCallScopeRef,
            @PublicContext AccessContext accessContextRef,
            TransactionMgrGenerator transactionMgrGeneratorRef
        )
    {
        errorReporter = errorReporterRef;
        apiCallScope = apiCallScopeRef;
        publicContext = accessContextRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    Context createContext(String apiCall, org.glassfish.grizzly.http.server.Request request)
    {
        // request.getAuthorization() contains authorization http field

        final String userAgent = request.getHeader("User-Agent");
        Peer peer = new PeerREST(request.getRemoteAddr(), userAgent);
        errorReporter.logDebug("REST access api '%s' from '%s'", apiCall, peer.toString());
        return  Context.of(
            ApiModule.API_CALL_NAME, apiCall,
            AccessContext.class, publicContext,
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
                    catch (SQLException sqlExc)
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
