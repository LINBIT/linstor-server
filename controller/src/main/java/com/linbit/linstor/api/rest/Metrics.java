package com.linbit.linstor.api.rest;

import com.linbit.linstor.api.rest.v1.RequestHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.prometheus.PrometheusBuilder;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("metrics")
public class Metrics
{
    private final ErrorReporter errorReporter;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final CtrlErrorListApiCallHandler ctrlErrorListApiCallHandler;
    private final PrometheusBuilder prometheusBuilder;

    private static final int BLOCK_TIMEOUT = 10;
    private static final AtomicLong SCRAPE_REQUESTS = new AtomicLong();

    @Inject
    public Metrics(
        ErrorReporter errorReporterRef,
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef,
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef,
        CtrlErrorListApiCallHandler ctrlErrorListApiCallHandlerRef,
        PrometheusBuilder prometheusBuilderRef)
    {
        errorReporter = errorReporterRef;
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        ctrlErrorListApiCallHandler = ctrlErrorListApiCallHandlerRef;
        prometheusBuilder = prometheusBuilderRef;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN + "; version=0.0.4")
    public Response metrics(
        @Context Request request,
        @DefaultValue("true") @QueryParam("resource") boolean resources,
        @DefaultValue("true") @QueryParam("storage_pools") boolean storagePools,
        @DefaultValue("true") @QueryParam("error_reports") boolean withErrorReports
    )
    {
        MDC.put(ErrorReporter.LOGID, ErrorReporter.getNewLogId());
        SCRAPE_REQUESTS.incrementAndGet();
        long scrapeStart = System.currentTimeMillis();

        ErrorReportResult errorReportsTmp = null;
        if (withErrorReports)
        {
            Flux<ErrorReportResult> fluxErrorReports = ctrlErrorListApiCallHandler.listErrorReports(
                Collections.emptySet(), false, null, null, Collections.emptySet(), 1L, 0L)
                .contextWrite(requestHelper.createContext("metrics", request));

            try
            {
                long start = System.currentTimeMillis();
                errorReportsTmp = fluxErrorReports
                    .timeout(Duration.ofSeconds(BLOCK_TIMEOUT)).next().block();
                errorReporter.logTrace("Metric/ListErrorReports: %dms", System.currentTimeMillis() - start);
            }
            catch (RuntimeException runtimeExc)
            {
                if (runtimeExc.getCause() instanceof TimeoutException)
                {
                    errorReporter.logWarning(
                        String.format("Timeout: Gathering error reports took longer than %d seconds: %s",
                            BLOCK_TIMEOUT,
                            runtimeExc));
                }
                else
                {
                    errorReporter.reportError(runtimeExc);
                }
            }
        }

        final ErrorReportResult errorReports = errorReportsTmp;
        return requestHelper.doInScope("metrics", request, () ->
            {
                final ResourceList rl = resources ? ctrlVlmListApiCallHandler.listVlmsCached(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList()) : null;

                final List<StorPoolApi> storagePoolList = storagePools ?
                    ctrlStorPoolListApiCallHandler.listStorPoolsCached(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()) : null;

                final List<NodeApi> nodeApiList = ctrlApiCallHandler.listNodes(
                    Collections.emptyList(), Collections.emptyList());
                final List<ResourceDefinitionApi> rscDfns = ctrlApiCallHandler.listResourceDefinitions();
                final String promText = prometheusBuilder.build(
                    nodeApiList,
                    rscDfns,
                    rl,
                    storagePoolList,
                    errorReports,
                    SCRAPE_REQUESTS.getAndIncrement(),
                    scrapeStart
                );

                return Response.status(Response.Status.OK).entity(promText).build();
            },
            false);
    }
}
