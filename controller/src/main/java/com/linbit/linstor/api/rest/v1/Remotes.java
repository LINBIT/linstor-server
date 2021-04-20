package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("v1/remotes")
@Produces(MediaType.APPLICATION_JSON)
public class Remotes
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;

    private final CtrlRemoteApiCallHandler remoteHandler;

    @Inject
    Remotes(
        RequestHelper requestHelperRef,
        CtrlRemoteApiCallHandler remoteHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        remoteHandler = remoteHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response getRemotes(@Context Request request)
    {
        return requestHelper.doInScope(
            requestHelper.createContext(ApiConsts.API_LST_REMOTE, request),
            () ->
            {
                List<S3RemotePojo> remotePojoList = remoteHandler.listS3();
                List<JsonGenTypes.S3Remote> remotes = remotePojoList.stream()
                    .map(pojo -> Json.apiToS3Remote(pojo))
                    .collect(Collectors.toList());
                return RequestHelper.queryRequestResponse(
                    objectMapper,
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    null,
                    null,
                    remotes
                );
            },
            false
        );
    }

    @POST
    @Path("s3")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createS3Remote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.S3Remote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.S3Remote.class);
            Flux<ApiCallRc> flux = remoteHandler.createS3(
                remoteJson.remote_name,
                remoteJson.endpoint,
                remoteJson.bucket,
                remoteJson.region,
                remoteJson.access_key,
                remoteJson.secret_key
            ).subscriberContext(
                requestHelper.createContext(ApiConsts.API_SET_REMOTE, request)
            );

            requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PUT
    @Path("s3/{remoteName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void changeS3Remote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.S3Remote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.S3Remote.class);
            Flux<ApiCallRc> flux = remoteHandler.changeS3(
                remoteName,
                remoteJson.endpoint,
                remoteJson.bucket,
                remoteJson.region,
                remoteJson.access_key,
                remoteJson.secret_key
            ).subscriberContext(
                requestHelper.createContext(ApiConsts.API_SET_REMOTE, request)
            );

            requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @DELETE
    public void deleteRemote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @QueryParam("remote_name") String remoteName
    )
    {
        Flux<ApiCallRc> flux = remoteHandler.delete(remoteName)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_SET_REMOTE, request));
        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
    }
}
