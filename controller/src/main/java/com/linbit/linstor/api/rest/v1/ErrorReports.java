package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
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
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("v1/error-reports")
@Produces(MediaType.APPLICATION_JSON)
public class ErrorReports
{
    private final RequestHelper requestHelper;
    private final CtrlErrorListApiCallHandler ctrlErrorListApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    ErrorReports(
        RequestHelper requestHelperRef,
        CtrlErrorListApiCallHandler ctrlErrorListApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlErrorListApiCallHandler = ctrlErrorListApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public void listErrorReports(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @QueryParam("node") String nodeName,
        @QueryParam("since") Long since,
        @QueryParam("to") Long to,
        @DefaultValue("false") @QueryParam("withContent") boolean withContent,
        @DefaultValue("1000") @QueryParam("limit") long limit,
        @DefaultValue("0") @QueryParam("offset") long offset // is currently ignored, because it isn't working
    )
    {
        listErrorReports(
            request,
            asyncResponse,
            nodeName,
            null,
            since,
            to,
            withContent,
            limit,
            offset
        );
    }

    @GET
    @Path("{reportId}")
    public void listErrorReports(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @QueryParam("node") String nodeName,
        @PathParam("reportId") @Nullable String reportId,
        @QueryParam("since") Long since,
        @QueryParam("to") Long to,
        @DefaultValue("true") @QueryParam("withContent") boolean withContent,
        @DefaultValue("1000") @QueryParam("limit") long limit,
        @DefaultValue("0") @QueryParam("offset") long offset // is currently ignored, because it isn't working
    )
    {
        @Nullable Instant optSince = since != null ? Instant.ofEpochMilli(since) : null;
        @Nullable Instant optTo = to != null ? Instant.ofEpochMilli(to) : null;
        Set<String> filterNodes = new HashSet<>();
        Set<String> filterIds = new HashSet<>();

        if (nodeName != null)
        {
            filterNodes.add(nodeName);
        }

        if (reportId != null)
        {
            filterIds.add(reportId);
        }
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            Mono<Response> flux = ctrlErrorListApiCallHandler.listErrorReports(
                    filterNodes,
                    withContent,
                    optSince,
                    optTo,
                    filterIds,
                    limit,
                    offset)
                .flatMap(errorReportResult ->
                {
                    Stream<ErrorReport> finalStream = errorReportResult.getErrorReports().stream();
                    if (limit > 0)
                    {
                        finalStream = finalStream.limit(limit);
                    }
                    List<JsonGenTypes.ErrorReport> jsonReports = finalStream.map(errorReport ->
                        {
                            JsonGenTypes.ErrorReport jsonErrorReport = new JsonGenTypes.ErrorReport();
                            jsonErrorReport.node_name = errorReport.getNodeName();
                            jsonErrorReport.error_time = errorReport.getDateTime().toEpochMilli();
                            jsonErrorReport.filename = errorReport.getFileName();
                            jsonErrorReport.module = errorReport.getModuleString();
                            jsonErrorReport.version = errorReport.getVersion().orElse(null);
                            jsonErrorReport.peer = errorReport.getPeer().orElse(null);
                            jsonErrorReport.exception = errorReport.getException().orElse(null);
                            jsonErrorReport.exception_message = errorReport.getExceptionMessage().orElse(null);
                            jsonErrorReport.origin_file = errorReport.getOriginFile().orElse(null);
                            jsonErrorReport.origin_method = errorReport.getOriginMethod().orElse(null);
                            jsonErrorReport.origin_line = errorReport.getOriginLine().orElse(null);
                            jsonErrorReport.text = errorReport.getText().orElse(null);

                            return jsonErrorReport;
                        })
                        .collect(Collectors.toList());

                    Response resp;
                    try
                    {
                        resp = Response.status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(jsonReports))
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .build();
                    }
                    catch (JsonProcessingException exc)
                    {
                        exc.printStackTrace();
                        resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                    }
                    return Mono.just(resp);
                })
                .next();

            requestHelper.doFlux(
                ApiConsts.API_REQ_ERROR_REPORTS,
                request,
                asyncResponse,
                flux
            );
        }
    }

    @PATCH
    @Consumes(MediaType.APPLICATION_JSON)
    public void deleteErrorReports(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData)
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.ErrorReportDelete data = objectMapper.readValue(
                jsonData, JsonGenTypes.ErrorReportDelete.class);

            @Nullable Instant optSince = data.since != null ? Instant.ofEpochMilli(data.since) : null;
            @Nullable Instant optTo = data.to != null ? Instant.ofEpochMilli(data.to) : null;
            Flux<ApiCallRc> flux = ctrlErrorListApiCallHandler
                .deleteErrorReports(optSince, optTo, data.nodes, data.exception, data.version, data.ids);

            requestHelper.doFlux(
                ApiConsts.API_DEL_ERROR_REPORTS,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @DELETE
    @Path("{reportId}")
    public void deleteErrorReport(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("reportId") String reportId)
    {
        Flux<ApiCallRc> flux = ctrlErrorListApiCallHandler.deleteErrorReports(
                null, null, null, null, null, Collections.singletonList(reportId));

        requestHelper.doFlux(
            ApiConsts.API_DEL_ERROR_REPORT,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux)
        );
    }
}
