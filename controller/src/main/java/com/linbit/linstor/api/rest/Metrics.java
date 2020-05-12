package com.linbit.linstor.api.rest;

import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.api.rest.v1.RequestHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.logging.ErrorReport;
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
import java.util.concurrent.atomic.AtomicLong;

import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("metrics")
public class Metrics {
    private final ErrorReporter errorReporter;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final CtrlErrorListApiCallHandler ctrlErrorListApiCallHandler;
    private final PrometheusBuilder prometheusBuilder;

    private static final AtomicLong scrape_requests = new AtomicLong();

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
        scrape_requests.incrementAndGet();
        long scrape_start = System.currentTimeMillis();
        Flux<ResourceList> fluxVlms = ctrlVlmListApiCallHandler.listVlms(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList()).subscriberContext(requestHelper.createContext("metrics", request));

        ResourceList rlTmp = null;
        if (resources) {
            try {
                long start = System.currentTimeMillis();
                rlTmp = fluxVlms.next().block(Duration.ofSeconds(5));
                errorReporter.logTrace("Metric/ListVlms: %dms", System.currentTimeMillis() - start);
            } catch (RuntimeException timeoutExc) {
                errorReporter.reportError(
                    new LinStorRuntimeException("Gathering resource stats took longer than 5 seconds", timeoutExc));
            }
        }

        List<StorPoolApi> storagePoolListTmp = null;
        if (storagePools) {
            Flux<List<StorPoolApi>> fluxStorPools = ctrlStorPoolListApiCallHandler
                .listStorPools(Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
                .subscriberContext(requestHelper.createContext("metrics", request));

            try {
                long start = System.currentTimeMillis();
                storagePoolListTmp = fluxStorPools.next().block(Duration.ofSeconds(5));
                errorReporter.logTrace("Metric/ListStorPools: %dms", System.currentTimeMillis() - start);
            } catch (RuntimeException timeoutExc) {
                errorReporter.reportError(
                    new LinStorRuntimeException("Gathering storage pool stats took longer than 5 seconds", timeoutExc));
            }
        }

        List<ErrorReport> errorReportsTmp = null;
        if (withErrorReports) {
            Flux<List<ErrorReport>> fluxErrorReports = ctrlErrorListApiCallHandler.listErrorReports(
                Collections.emptySet(), false, null, null, Collections.emptySet())
                .subscriberContext(requestHelper.createContext("metrics", request));

            try {
                long start = System.currentTimeMillis();
                errorReportsTmp = fluxErrorReports.next().block(Duration.ofSeconds(5));
                errorReporter.logTrace("Metric/ListErrorReports: %dms", System.currentTimeMillis() - start);
            } catch (RuntimeException timeoutExc) {
                errorReporter.reportError(
                    new LinStorRuntimeException("Gathering error reports took longer than 5 seconds", timeoutExc));
            }
        }

        final ResourceList rl = rlTmp;
        final List<StorPoolApi> storagePoolList = storagePoolListTmp;
        final List<ErrorReport> errorReports = errorReportsTmp;
        return requestHelper.doInScope(requestHelper.createContext("metrics", request), () ->
            {
                final List<NodeApi> nodeApiList = ctrlApiCallHandler.listNodes(
                    Collections.emptyList(), Collections.emptyList());
                final List<ResourceDefinitionApi> rscDfns = ctrlApiCallHandler.listResourceDefinitions();
                final String promText = prometheusBuilder.build(
                    nodeApiList,
                    rscDfns,
                    rl,
                    storagePoolList,
                    errorReports,
                    scrape_requests.getAndIncrement(),
                    scrape_start
                );

                return Response.status(Response.Status.OK).entity(promText).build();
            },
            false);
    }
}
