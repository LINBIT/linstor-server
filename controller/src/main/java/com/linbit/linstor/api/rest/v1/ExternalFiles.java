package com.linbit.linstor.api.rest.v1;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExternalFile;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlExternalFilesApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
import java.io.UnsupportedEncodingException;
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
    private final ErrorReporter errorReporter;

    private final CtrlExternalFilesApiCallHandler extFilesHandler;

    @Inject
    ExternalFiles(
        RequestHelper requestHelperRef,
        ErrorReporter errorReporterRef,
        CtrlExternalFilesApiCallHandler extFilesHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        errorReporter = errorReporterRef;
        extFilesHandler = extFilesHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response getFiles(@Context Request request, @QueryParam("content") Boolean showContent)
    {
        return requestHelper.doInScope(
            requestHelper.createContext(ApiConsts.API_LST_EXT_FILES, request),
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
        try
        {
            String decodedExtFileName = URLDecoder.decode(extFileName, StandardCharsets.UTF_8.displayName());

            return requestHelper.doInScope(
                requestHelper.createContext(ApiConsts.API_LST_EXT_FILES, request),
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
        catch (UnsupportedEncodingException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @GET
    @Path("{extFileName}/check/{node}")
    public Response getFileAllowed(
        @Context Request request,
        @PathParam("extFileName") String extFileName,
        @PathParam("node") String nodeName
    )
    {
        try
        {
            String decodedExtFileName = URLDecoder.decode(extFileName, StandardCharsets.UTF_8.displayName());

            return requestHelper.doInScope(
                requestHelper.createContext(ApiConsts.API_CHECK_EXT_FILE, request),
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
        catch (UnsupportedEncodingException exc)
        {
            throw new ImplementationError(exc);
        }
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
                URLDecoder.decode(extFileName, StandardCharsets.UTF_8.displayName()),
                Base64.decode(extFileJson.content)
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
        try
        {
            Flux<ApiCallRc> flux = extFilesHandler
                .delete(URLDecoder.decode(extFileName, StandardCharsets.UTF_8.displayName()));
            requestHelper.doFlux(
                ApiConsts.API_DEL_EXT_FILE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (UnsupportedEncodingException exc)
        {
            throw new ImplementationError(exc);
        }
    }
}
