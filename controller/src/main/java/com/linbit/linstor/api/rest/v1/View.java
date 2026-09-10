package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.backups.BackupNodeQueuesPojo;
import com.linbit.linstor.api.pojo.backups.BackupSnapQueuesPojo;
import com.linbit.linstor.api.pojo.backups.ScheduleDetailsPojo;
import com.linbit.linstor.api.pojo.backups.ScheduledRscsPojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlScheduleApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlBackupQueueInternalCallHandler;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReportSortBy;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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

@Path("v1/view")
@Produces(MediaType.APPLICATION_JSON)
public class View
{
    private static final int ERROR_REPORT_PAGE_MAX_LIMIT = 10_000;

    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlScheduleApiCallHandler ctrlScheduleApiCallHandler;
    private final CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandler;
    private final CtrlErrorListApiCallHandler ctrlErrorListApiCallHandler;

    @Inject
    View(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef,
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef,
        CtrlScheduleApiCallHandler ctrlScheduleApiCallHandlerRef,
        CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandlerRef,
        CtrlErrorListApiCallHandler ctrlErrorListApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        ctrlScheduleApiCallHandler = ctrlScheduleApiCallHandlerRef;
        ctrlBackupQueueHandler = ctrlBackupQueueHandlerRef;
        ctrlErrorListApiCallHandler = ctrlErrorListApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }


    @GET
    @Path("resources")
    public void viewResources(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @QueryParam("nodes") List<String> nodes,
        @QueryParam("resources") List<String> resources,
        @QueryParam("storage_pools") List<String> storagePools,
        @QueryParam("props") List<String> propFilters,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        List<String> nodesFilter = nodes != null ? nodes : Collections.emptyList();
        List<String> storagePoolsFilter = storagePools != null ? storagePools : Collections.emptyList();
        List<String> resourcesFilter = resources != null ? resources : Collections.emptyList();

        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            MDC.put(ErrorReporter.LOGID, ErrorReporter.getNewLogId());
            Flux<ResourceList> flux = ctrlVlmListApiCallHandler.listVlms(
                nodesFilter, storagePoolsFilter, resourcesFilter, propFilters);

            requestHelper.doFlux(
                ApiConsts.API_LST_VLM,
                request,
                asyncResponse,
                listVolumesApiCallRcWithToResponse(flux, limit, offset)
            );
        });
    }

    private Mono<Response> listVolumesApiCallRcWithToResponse(
        Flux<ResourceList> resourceListFlux,
        int limit,
        int offset
    )
    {
        return resourceListFlux.flatMap(resourceList ->
        {
            Response resp;

            Stream<ResourceApi> rscApiStream = resourceList.getResources().stream();

            if (limit > 0)
            {
                rscApiStream = rscApiStream.skip(offset).limit(limit);
            }

            final List<JsonGenTypes.Resource> rscs = rscApiStream
                .map(rscApi -> Json.apiToResourceWithVolumes(rscApi, resourceList.getSatelliteStates(), true))
                .collect(Collectors.toList());

            try
            {
                resp = Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(rscs))
                    .build();
            }
            catch (JsonProcessingException exc)
            {
                exc.printStackTrace();
                resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }

            return Mono.just(resp);
        }).next();
    }

    @GET
    @Path("storage-pools")
    public void viewStoragePools(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @QueryParam("nodes") List<String> nodes,
        @QueryParam("storage_pools") List<String> storagePools,
        @QueryParam("props") List<String> propFilters,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset,
        @DefaultValue("false") @QueryParam("cached") boolean fromCache
    )
    {
        List<String> nodesFilter = nodes != null ? nodes : Collections.emptyList();
        List<String> storagePoolsFilter = storagePools != null ? storagePools : Collections.emptyList();

        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            MDC.put(ErrorReporter.LOGID, ErrorReporter.getNewLogId());
            Flux<List<StorPoolApi>> flux = ctrlStorPoolListApiCallHandler
                .listStorPools(nodesFilter, storagePoolsFilter, propFilters, fromCache);

            requestHelper.doFlux(
                ApiConsts.API_LST_STOR_POOL,
                request,
                asyncResponse,
                storPoolListToResponse(flux, limit, offset)
            );
        });
    }

    private Mono<Response> storPoolListToResponse(
        Flux<List<StorPoolApi>> storPoolListFlux,
        int limit,
        int offset
    )
    {
        return storPoolListFlux.flatMap(storPoolList ->
        {
            Response resp;
            Stream<StorPoolApi> storPoolApiStream = storPoolList.stream();
            if (limit > 0)
            {
                storPoolApiStream = storPoolApiStream.skip(offset).limit(limit);
            }
            List<JsonGenTypes.StoragePool> storPoolDataList = storPoolApiStream
                .map(Json::storPoolApiToStoragePool)
                .collect(Collectors.toList());

            try
            {
                resp = Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(storPoolDataList))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
            }
            catch (JsonProcessingException exc)
            {
                exc.printStackTrace();
                resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }

            return Mono.just(resp);
        }).next();
    }

    @GET
    @Path("snapshots")
    public Response listSnapshots(
        @Context Request request,
        @QueryParam("nodes") List<String> nodes,
        @QueryParam("resources") List<String> resources,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_SNAPSHOT_DFN, request, () ->
        {
            List<String> nodesFilter = nodes != null ? nodes : Collections.emptyList();
            // name filters are matched case-insensitively and may be regular expressions, so no normalization needed
            List<String> resourcesFilter = resources != null ? resources : Collections.emptyList();

            Response response;

            Stream<SnapshotDefinitionListItemApi> snapsStream =
                ctrlApiCallHandler.listSnapshotDefinition(nodesFilter, resourcesFilter).stream();

            if (limit > 0)
            {
                snapsStream = snapsStream.skip(offset).limit(limit);
            }

            List<JsonGenTypes.Snapshot> snapshot = snapsStream
                .map(Json::apiToSnapshot)
                .collect(Collectors.toList());

            response = RequestHelper.queryRequestResponse(
                objectMapper, ApiConsts.FAIL_NOT_FOUND_SNAPSHOT, "Snapshot", null, snapshot
            );

            return response;
        }, false);
    }

    @GET
    @Path("error-reports")
    public void viewErrorReports(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @QueryParam("node") List<String> nodes,
        @Nullable @QueryParam("since") Long since,
        @Nullable @QueryParam("to") Long to,
        @DefaultValue("false") @QueryParam("withContent") boolean withContent,
        @Nullable @QueryParam("module") String module,
        @DefaultValue("1000") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") long offset,
        @DefaultValue("error_time") @QueryParam("sort_by") String sortByRef,
        @DefaultValue("desc") @QueryParam("sort_order") String sortOrderRef
    )
    {
        ApiCallRcImpl paramErrors = new ApiCallRcImpl();

        @Nullable ErrorReportSortBy sortBy = ErrorReportSortBy.parse(sortByRef);
        if (sortBy == null)
        {
            paramErrors.addEntry(ApiCallRcImpl.entryBuilder(
                ApiConsts.API_CALL_PARSE_ERROR,
                "Invalid sort_by value: " + sortByRef
            ).setCorrection(
                "Use one of: " + Arrays.stream(ErrorReportSortBy.values())
                    .map(ErrorReportSortBy::getApiValue)
                    .collect(Collectors.joining(", "))
            ).build());
        }

        final boolean sortAsc = "asc".equalsIgnoreCase(sortOrderRef);
        if (!sortAsc && !"desc".equalsIgnoreCase(sortOrderRef))
        {
            paramErrors.addEntry(ApiCallRcImpl.entryBuilder(
                ApiConsts.API_CALL_PARSE_ERROR,
                "Invalid sort_order value: " + sortOrderRef
            ).setCorrection("Use asc or desc").build());
        }

        @Nullable Node.Type moduleFilter = null;
        if (module != null && !module.isEmpty())
        {
            if (Node.Type.CONTROLLER.name().equalsIgnoreCase(module))
            {
                moduleFilter = Node.Type.CONTROLLER;
            }
            else if (Node.Type.SATELLITE.name().equalsIgnoreCase(module))
            {
                moduleFilter = Node.Type.SATELLITE;
            }
            else
            {
                paramErrors.addEntry(ApiCallRcImpl.entryBuilder(
                    ApiConsts.API_CALL_PARSE_ERROR,
                    "Invalid module value: " + module
                ).setCorrection(
                    "Use " + Node.Type.CONTROLLER.name() + " or " + Node.Type.SATELLITE.name()
                ).build());
            }
        }

        if (limit <= 0 || limit > ERROR_REPORT_PAGE_MAX_LIMIT)
        {
            paramErrors.addEntry(ApiCallRcImpl.entryBuilder(
                ApiConsts.API_CALL_PARSE_ERROR,
                "Invalid limit value: " + limit
            ).setCorrection("Use a limit between 1 and " + ERROR_REPORT_PAGE_MAX_LIMIT).build());
        }

        if (offset < 0)
        {
            paramErrors.addEntry(ApiCallRcImpl.entryBuilder(
                ApiConsts.API_CALL_PARSE_ERROR,
                "Invalid offset value: " + offset
            ).setCorrection("Use an offset >= 0").build());
        }

        if (!paramErrors.isEmpty())
        {
            // do not use ApiCallRcRestUtils.toResponse here, since that would turn the error
            // entries into a 500 instead of a 400
            asyncResponse.resume(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(ApiCallRcRestUtils.toJSONCatch(paramErrors))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .build()
            );
        }
        else
        {
            @Nullable Instant optSince = since != null ? Instant.ofEpochMilli(since) : null;
            @Nullable Instant optTo = to != null ? Instant.ofEpochMilli(to) : null;
            Set<String> nodesFilter = nodes != null ? new HashSet<>(nodes) : Collections.emptySet();

            final int pageLimit = limit;
            final long pageOffset = offset;
            try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
            {
                Mono<Response> answer = ctrlErrorListApiCallHandler.listErrorReportsPage(
                        nodesFilter,
                        withContent,
                        optSince,
                        optTo,
                        moduleFilter,
                        pageLimit,
                        pageOffset,
                        sortBy,
                        sortAsc)
                    .flatMap(pageResult ->
                    {
                        JsonGenTypes.ErrorReportPage jsonPage = new JsonGenTypes.ErrorReportPage();
                        jsonPage.total = pageResult.getTotalCount();
                        jsonPage.limit = pageLimit;
                        jsonPage.offset = pageOffset;
                        jsonPage.sort_by = sortBy.getApiValue();
                        jsonPage.sort_order = sortAsc ? "asc" : "desc";
                        jsonPage.items = pageResult.getErrorReports().stream()
                            .map(Json::errorReportToJson)
                            .collect(Collectors.toList());

                        Response resp;
                        try
                        {
                            resp = Response.status(Response.Status.OK)
                                .entity(objectMapper.writeValueAsString(jsonPage))
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
                    answer
                );
            }
        }
    }

    @GET
    @Path("schedules-by-resource")
    public Response listActiveRscs(
        @Context Request request,
        @Nullable @QueryParam("rsc") String rscName,
        @Nullable @QueryParam("remote") String remoteName,
        @Nullable @QueryParam("schedule") String scheduleName,
        @QueryParam("active-only") @DefaultValue("false") boolean activeOnly
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_SCHEDULE,
            request,
            () ->
            {
                List<ScheduledRscsPojo> activeList = ctrlScheduleApiCallHandler
                    .listScheduledRscs(rscName, remoteName, scheduleName, activeOnly);
                List<JsonGenTypes.ScheduledRscs> jsonList = new ArrayList<>();
                for (ScheduledRscsPojo pojo : activeList)
                {
                    jsonList.add(Json.apiToScheduledRscs(pojo));
                }
                JsonGenTypes.ScheduledRscsList json = new JsonGenTypes.ScheduledRscsList();
                json.data = jsonList;
                return Response.status(Response.Status.OK).entity(objectMapper.writeValueAsString(json))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("schedules-by-resource/{rscName}")
    public Response listScheduleDetails(
        @Context Request request,
        @PathParam("rscName") String rscName
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_SCHEDULE,
            request,
            () ->
            {
                List<ScheduleDetailsPojo> detailsList = ctrlScheduleApiCallHandler.listScheduleDetails(rscName);
                List<JsonGenTypes.ScheduleDetails> jsonList = new ArrayList<>();
                for (ScheduleDetailsPojo detail : detailsList)
                {
                    jsonList.add(Json.apiToScheduleDetails(detail));
                }
                JsonGenTypes.ScheduleDetailsList json = new JsonGenTypes.ScheduleDetailsList();
                json.data = jsonList;
                return Response.status(Response.Status.OK).entity(objectMapper.writeValueAsString(json)).build();
            },
            false
        );
    }

    @GET
    @Path("backup/queue")
    public Response listBackupQueues(
        @Context Request request,
        @QueryParam("nodes") List<String> nodes,
        @QueryParam("snapshots") List<String> snapshots,
        @QueryParam("resources") List<String> resources,
        @QueryParam("remotes") List<String> remotes,
        @QueryParam("snap_to_node") boolean snapToNode
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_QUEUE,
            request,
            () ->
            {
                JsonGenTypes.BackupQueues json = new JsonGenTypes.BackupQueues();
                if (snapToNode)
                {
                    List<BackupSnapQueuesPojo> queues = ctrlBackupQueueHandler.listSnapQueues(
                        nodes,
                        snapshots,
                        resources,
                        remotes
                    );
                    json.snap_queues = new ArrayList<>();
                    for (BackupSnapQueuesPojo queue : queues)
                    {
                        json.snap_queues.add(Json.apiToSnapQueues(queue));
                    }
                }
                else
                {
                    List<BackupNodeQueuesPojo> queues = ctrlBackupQueueHandler.listNodeQueues(
                        nodes,
                        snapshots,
                        resources,
                        remotes
                    );
                    json.node_queues = new ArrayList<>();
                    for (BackupNodeQueuesPojo queue : queues)
                    {
                        json.node_queues.add(Json.apiToNodeQueues(queue));
                    }
                }
                return Response.status(Response.Status.OK).entity(objectMapper.writeValueAsString(json)).build();
            },
            false
        );
    }
}
