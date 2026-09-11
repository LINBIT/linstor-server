package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
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
            ApiConsts.API_LST_REMOTE,
            request,
            () ->
            {
                JsonGenTypes.RemoteList remoteList = new JsonGenTypes.RemoteList();
                remoteList.s3_remotes = remoteHandler.listS3().stream()
                    .map(Json::apiToS3Remote)
                    .collect(Collectors.toList());
                remoteList.linstor_remotes = remoteHandler.listLinstor().stream()
                    .map(Json::apiToLinstorRemote)
                    .collect(Collectors.toList());
                remoteList.ebs_remotes = remoteHandler.listEbs().stream()
                    .map(Json::apiToEbsRemote)
                    .collect(Collectors.toList());

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(remoteList))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("s3")
    public Response getS3Remotes(@Context Request request)
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_REMOTE,
            request,
            () ->
            {
                List<JsonGenTypes.S3Remote> remoteList = remoteHandler.listS3().stream()
                    .map(Json::apiToS3Remote)
                    .collect(Collectors.toList());

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(remoteList))
                    .build();
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.S3Remote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.S3Remote.class);
            Flux<ApiCallRc> flux = remoteHandler.createS3(
                remoteJson.remote_name,
                remoteJson.endpoint,
                remoteJson.bucket,
                remoteJson.region,
                remoteJson.access_key,
                remoteJson.secret_key,
                remoteJson.use_path_style
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_REMOTE,
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.S3Remote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.S3Remote.class);
            Flux<ApiCallRc> flux = remoteHandler.changeS3(
                remoteName,
                remoteJson.endpoint,
                remoteJson.bucket,
                remoteJson.region,
                remoteJson.access_key,
                remoteJson.secret_key
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_REMOTE,
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

    @GET
    @Path("linstor")
    public Response getLinstorRemotes(@Context Request request)
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_REMOTE,
            request,
            () ->
            {
                List<JsonGenTypes.LinstorRemote> remoteList = remoteHandler.listLinstor().stream()
                    .map(Json::apiToLinstorRemote)
                    .collect(Collectors.toList());

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(remoteList))
                    .build();
            },
            false
        );
    }

    @POST
    @Path("linstor")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createLinstorRemote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.LinstorRemote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.LinstorRemote.class);
            Flux<ApiCallRc> flux = remoteHandler.createLinstor(
                remoteJson.remote_name,
                remoteJson.url,
                remoteJson.passphrase,
                remoteJson.cluster_id
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_REMOTE,
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

    @PUT
    @Path("linstor/{remoteName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void changeLinstorRemote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.LinstorRemote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.LinstorRemote.class);
            Flux<ApiCallRc> flux = remoteHandler.changeLinstor(
                remoteName,
                remoteJson.url,
                remoteJson.passphrase,
                remoteJson.cluster_id
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_REMOTE,
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

    @GET
    @Path("ebs")
    public Response getEbsRemotes(@Context Request request)
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_REMOTE,
            request,
            () ->
            {
                List<JsonGenTypes.EbsRemote> remoteList = remoteHandler.listEbs().stream()
                    .map(Json::apiToEbsRemote)
                    .collect(Collectors.toList());

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(remoteList))
                    .build();
            },
            false
        );
    }

    @POST
    @Path("ebs")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createEbsRemote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.EbsRemote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.EbsRemote.class);
            Flux<ApiCallRc> flux = remoteHandler.createEbs(
                remoteJson.remote_name,
                remoteJson.endpoint,
                remoteJson.region,
                remoteJson.availability_zone,
                remoteJson.access_key,
                remoteJson.secret_key
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_REMOTE,
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

    @PUT
    @Path("ebs/{remoteName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void changeEbsRemote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.EbsRemote remoteJson = objectMapper.readValue(jsonData, JsonGenTypes.EbsRemote.class);
            Flux<ApiCallRc> flux = remoteHandler.changeEbs(
                remoteName,
                remoteJson.endpoint,
                remoteJson.region,
                remoteJson.availability_zone,
                remoteJson.access_key,
                remoteJson.secret_key
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_REMOTE,
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
    public void deleteRemote(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @QueryParam("remote_name") String remoteName
    )
    {
        Flux<ApiCallRc> flux = remoteHandler.delete(remoteName);
        requestHelper.doFlux(
            ApiConsts.API_SET_REMOTE,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }
}
