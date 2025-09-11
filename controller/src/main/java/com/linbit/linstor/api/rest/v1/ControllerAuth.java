package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAuthHandler;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("v1/controller/auth")
@Produces(MediaType.APPLICATION_JSON)
public class ControllerAuth
{
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlAuthHandler ctrlAuthHandler;

    @Inject
    public ControllerAuth(
        RequestHelper requestHelperRef,
        CtrlAuthHandler ctrlAuthHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlAuthHandler = ctrlAuthHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @POST
    @Path("initialize-token-auth")
    @Consumes(MediaType.APPLICATION_JSON)
    public void initializeTokenAuth(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.InitAuthTokenRequest initRequest = objectMapper.readValue(
                jsonData,
                JsonGenTypes.InitAuthTokenRequest.class
            );

            Flux<ApiCallRc> flux = ctrlAuthHandler.initializeTokenAuth(
                initRequest.only_satellites,
                initRequest.description,
                initRequest.no_https
            );

            requestHelper.doFlux(
                ApiConsts.API_CTRL_INIT_AUTH_TOKEN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @GET
    @Path("token")
    public Response listAuthTokens(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_CTRL_LIST_AUTH_TOKEN, request,
            () ->
            {
                JsonGenTypes.AuthTokenListResponse response = new JsonGenTypes.AuthTokenListResponse();
                response.list = ctrlAuthHandler.listTokens();
                response.count = response.list.size();
                return Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(response))
                    .build();
            },
            false
        );
    }

    @POST
    @Path("token")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createAuthToken(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.CreateAuthToken createTokenInput = objectMapper.readValue(
                jsonData,
                JsonGenTypes.CreateAuthToken.class
            );

            Flux<ApiCallRc> flux = ctrlAuthHandler.createToken(
                createTokenInput.description,
                createTokenInput.expires_at,
                createTokenInput.ip_filter
            );

            requestHelper.doFlux(
                ApiConsts.API_CTRL_CRT_AUTH_TOKEN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PUT
    @Path("token/{authtokenid}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void modifyAuthToken(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("authtokenid") int authTokenId,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.ModifyAuthToken tokenModify = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ModifyAuthToken.class
            );

            Flux<ApiCallRc> flux = ctrlAuthHandler.modifyToken(
                authTokenId,
                tokenModify.description,
                tokenModify.ip_filter,
                tokenModify.is_active
            );

            requestHelper.doFlux(
                ApiConsts.API_CTRL_MODIFY_AUTH_TOKEN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @DELETE
    @Path("token/{authtokenid}")
    public void revokeAuthToken(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("authtokenid") int authTokenId
    )
    {
        Flux<ApiCallRc> flux = ctrlAuthHandler.revokeToken(authTokenId);

        requestHelper.doFlux(
            ApiConsts.API_CTRL_DELETE_AUTH_TOKEN,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }
}
