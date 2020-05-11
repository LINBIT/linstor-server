package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.logging.ErrorReport;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
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
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
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
            0,
            0
        );
    }

    @GET
    @Path("{reportId}")
    public void listErrorReports(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @QueryParam("node") String nodeName,
        @PathParam("reportId") String reportId,
        @QueryParam("since") Long since,
        @QueryParam("to") Long to,
        @DefaultValue("true") @QueryParam("withContent") boolean withContent,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        Optional<Date> optSince = Optional.ofNullable(since != null ? new Date(since) : null);
        Optional<Date> optTo = Optional.ofNullable(to != null ? new Date(to) : null);
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

        Mono<Response> flux = ctrlErrorListApiCallHandler.listErrorReports(
                filterNodes,
                withContent,
                optSince,
                optTo,
                filterIds)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_REQ_ERROR_REPORTS, request))
            .flatMap(reportSet -> Flux.just(reportSet.stream()))
            .flatMap(errorReportStream ->
            {
                Stream<ErrorReport> finalStream = errorReportStream;
                if (limit > 0)
                {
                    finalStream = finalStream.skip(offset).limit(limit);
                }
                List<JsonGenTypes.ErrorReport> jsonReports = finalStream.map(errorReport ->
                {
                    JsonGenTypes.ErrorReport jsonErrorReport = new JsonGenTypes.ErrorReport();
                    jsonErrorReport.node_name = errorReport.getNodeName();
                    jsonErrorReport.error_time = errorReport.getDateTime().getTime();
                    jsonErrorReport.filename = errorReport.getFileName();
                    jsonErrorReport.text = errorReport.getText();

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

        requestHelper.doFlux(asyncResponse, flux);
    }
}
