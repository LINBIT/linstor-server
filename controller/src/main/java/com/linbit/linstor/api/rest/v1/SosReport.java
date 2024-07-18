package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSosReportApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.TimeUtils;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.io.ByteStreams;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Mono;

@Path("v1/sos-report")
@Produces(MediaType.APPLICATION_JSON)
public class SosReport
{
    private final RequestHelper requestHelper;
    private final CtrlSosReportApiCallHandler ctrlSosReportApiCallHandler;
    private static final int DEFAULT_DAYS = 7;

    @Inject
    public SosReport(
        RequestHelper reqestHelperRef,
        CtrlSosReportApiCallHandler ctrlSosReportApiCallHandlerRef
    )
    {
        requestHelper = reqestHelperRef;
        ctrlSosReportApiCallHandler = ctrlSosReportApiCallHandlerRef;
    }

    @GET
    @Path("/download")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public void downloadSosReport(
        @Context Request request,
        @QueryParam("nodes") List<String> nodeNames,
        @QueryParam("rscs") List<String> rscNames,
        @QueryParam("exclude") List<String> excludeNodes,
        @QueryParam("since") Long since,
        @QueryParam("include-ctrl") @DefaultValue("true") boolean includeCtrl,
        @Suspended final AsyncResponse asyncResponse
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            Set<String> filterNodes = new HashSet<>();
            if (nodeNames != null)
            {
                filterNodes.addAll(nodeNames);
            }
            Set<String> filterRscs = new HashSet<>();
            if (rscNames != null)
            {
                filterRscs.addAll(rscNames);
            }
            Set<String> filterExclude = new HashSet<>();
            if (excludeNodes != null)
            {
                filterExclude.addAll(excludeNodes);
            }
            final LocalDateTime sinceDate = since != null ?
                TimeUtils.millisToDate(since) :
                LocalDateTime.now().minus(DEFAULT_DAYS, ChronoUnit.DAYS);

            Mono<Response> flux = ctrlSosReportApiCallHandler
                .getSosReport(filterNodes, filterRscs, filterExclude, sinceDate, includeCtrl, request.getQueryString())
                .contextWrite(requestHelper.createContext(ApiConsts.API_REQ_SOS_REPORT, request))
                .flatMap(sosReport ->
                {
                    Response resp;
                    resp = Response
                        .ok((StreamingOutput) output ->
                        {
                            try
                            {
                                java.nio.file.Path path = Paths.get(sosReport);
                                FileInputStream input = new FileInputStream(path.toFile());
                                ByteStreams.copy(input, output);
                                output.flush();
                                input.close();
                            }
                            catch (Exception exc)
                            {
                                throw new WebApplicationException("File Not Found !!", exc);
                            }
                        }, MediaType.APPLICATION_OCTET_STREAM)
                        .header(
                            "content-disposition",
                            "attachment; filename = " + Paths.get(sosReport).getFileName().toString()
                        )
                        .build();
                    return Mono.just(resp);
                })
                .next();

            requestHelper.doFlux(asyncResponse, flux);
        }
    }

    @GET
    public void getSosReport(
        @Context
        Request request,
        @QueryParam("nodes") List<String> nodeNames,
        @QueryParam("rscs") List<String> rscNames,
        @QueryParam("exclude") List<String> excludeNodes,
        @QueryParam("since") Long since,
        @QueryParam("include-ctrl") @DefaultValue("true") boolean includeCtrl,
        @Suspended final AsyncResponse asyncResponse
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            Set<String> filterNodes = new HashSet<>();
            if (nodeNames != null)
            {
                filterNodes.addAll(nodeNames);
            }
            Set<String> filterRscs = new HashSet<>();
            if (rscNames != null)
            {
                filterRscs.addAll(rscNames);
            }
            Set<String> filterExclude = new HashSet<>();
            if (excludeNodes != null)
            {
                filterExclude.addAll(excludeNodes);
            }
            final LocalDateTime sinceDate = since != null ?
                TimeUtils.millisToDate(since) :
                LocalDateTime.now().minus(DEFAULT_DAYS, ChronoUnit.DAYS);

            Mono<Response> flux = ctrlSosReportApiCallHandler
                .getSosReport(filterNodes, filterRscs, filterExclude, sinceDate, includeCtrl, request.getQueryString())
                .contextWrite(requestHelper.createContext(ApiConsts.API_REQ_SOS_REPORT, request))
                .flatMap(sosReport ->
                {
                    ApiCallRcImpl apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.CREATED | ApiConsts.MASK_SUCCESS,
                            "SOS Report created on Controller: " + sosReport
                        ).putObjRef("path", sosReport).build());
                    return Mono.just(ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED));
                })
                .next();

            requestHelper.doFlux(asyncResponse, flux);
        }
    }
}
