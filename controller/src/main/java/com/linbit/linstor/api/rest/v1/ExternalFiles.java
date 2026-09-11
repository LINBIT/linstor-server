package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExternalFile;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlExternalFilesApiCallHandler;
import com.linbit.utils.Base64;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("v1/files")
@Produces(MediaType.APPLICATION_JSON)
public class ExternalFiles
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;

    private final CtrlExternalFilesApiCallHandler extFilesHandler;

    @Inject
    ExternalFiles(
        RequestHelper requestHelperRef,
        CtrlExternalFilesApiCallHandler extFilesHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        extFilesHandler = extFilesHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response getFiles(@Context Request request, @QueryParam("content") Boolean showContent)
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_EXT_FILES,
            request,
            () ->
            {
                List<ExternalFilePojo> extFilePojoList = extFilesHandler.listFiles(includeAll -> true);
                List<ExternalFile> extFiles = extFilePojoList.stream()
                    .map(pojo -> Json.apiToExternalFile(pojo, showContent != null && showContent))
                    .collect(Collectors.toList());
                return RequestHelper.queryRequestResponse(
                    objectMapper,
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    null,
                    null,
                    extFiles
                );
            },
            false
        );
    }

    @GET
    @Path("{extFileName}")
    public Response getFiles(@Context Request request, @PathParam("extFileName") String extFileName)
    {
        String decodedExtFileName = URLDecoder.decode(extFileName, StandardCharsets.UTF_8);

        return requestHelper.doInScope(
            ApiConsts.API_LST_EXT_FILES,
            request,
            () ->
            {
                List<ExternalFilePojo> extFilePojoList = extFilesHandler
                    .listFiles(arg -> arg.equalsIgnoreCase(decodedExtFileName));

                List<ExternalFile> extFiles = extFilePojoList.stream()
                    .map(pojo -> Json.apiToExternalFile(pojo, true))
                    .collect(Collectors.toList());
                return RequestHelper.queryRequestResponse(
                    objectMapper,
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "External file",
                    decodedExtFileName,
                    extFiles
                );
            },
            false
        );
    }

    @GET
    @Path("{extFileName}/check/{node}")
    public Response getFileAllowed(
        @Context Request request,
        @PathParam("extFileName") String extFileName,
        @PathParam("node") String nodeName
    )
    {
        String decodedExtFileName = URLDecoder.decode(extFileName, StandardCharsets.UTF_8);

        return requestHelper.doInScope(
            ApiConsts.API_CHECK_EXT_FILE,
            request,
            () ->
            {
                boolean allowed = extFilesHandler.checkFile(decodedExtFileName, nodeName);

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(Json.apiToExtFileCheckResult(allowed)))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("{extFileName}/status/{node}")
    public void getFileStatus(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("extFileName") String extFileName,
        @PathParam("node") String nodeName
    )
    {
        String decodedExtFileName = URLDecoder.decode(extFileName, StandardCharsets.UTF_8);

        requestHelper.doFlux(
            ApiConsts.API_LST_EXT_FILES,
            request,
            asyncResponse,
            extFilesHandler.getStatus(decodedExtFileName, nodeName)
                .map(status ->
                {
                    JsonGenTypes.ExtFileStatusResult json = new JsonGenTypes.ExtFileStatusResult();
                    json.actual_path = status.getActualPath();
                    json.content_match = status.isContentMatch();
                    try
                    {
                        return Response
                            .status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(json))
                            .type(MediaType.APPLICATION_JSON)
                            .build();
                    }
                    catch (IOException exc)
                    {
                        throw new RuntimeException(exc);
                    }
                })
                .switchIfEmpty(reactor.core.publisher.Mono.just(
                    Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity("{\"message\": \"Satellite not connected\"}")
                        .type(MediaType.APPLICATION_JSON)
                        .build()
                ))
        );
    }

    @PUT
    @Path("{extFileName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void putFile(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("extFileName") String extFileName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.ExternalFile extFileJson = objectMapper.readValue(jsonData, JsonGenTypes.ExternalFile.class);
            Flux<ApiCallRc> flux = extFilesHandler.set(
                URLDecoder.decode(extFileName, StandardCharsets.UTF_8),
                extFileJson.content == null ? null : Base64.decode(extFileJson.content),
                extFileJson.alt_suffixes
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_EXT_FILE,
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
    @Path("{extFileName}")
    public void deleteFile(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("extFileName") String extFileName
    )
    {
        Flux<ApiCallRc> flux = extFilesHandler
            .delete(URLDecoder.decode(extFileName, StandardCharsets.UTF_8));
        requestHelper.doFlux(
            ApiConsts.API_DEL_EXT_FILE,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }
}
