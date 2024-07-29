package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.backups.BackupNodeQueuesPojo;
import com.linbit.linstor.api.pojo.backups.BackupSnapQueuesPojo;
import com.linbit.linstor.api.pojo.backups.ScheduleDetailsPojo;
import com.linbit.linstor.api.pojo.backups.ScheduledRscsPojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlScheduleApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlBackupQueueInternalCallHandler;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.SnapshotShippingListItemApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.logging.ErrorReporter;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlScheduleApiCallHandler ctrlScheduleApiCallHandler;
    private final CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandler;

    @Inject
    View(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef,
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef,
        CtrlScheduleApiCallHandler ctrlScheduleApiCallHandlerRef,
        CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        ctrlScheduleApiCallHandler = ctrlScheduleApiCallHandlerRef;
        ctrlBackupQueueHandler = ctrlBackupQueueHandlerRef;
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
            List<String> resourcesFilter = resources != null ?
                resources.parallelStream().map(String::toLowerCase).collect(Collectors.toList()) :
                Collections.emptyList();

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

    @Deprecated(forRemoval = true)
    @GET
    @Path("snapshot-shippings")
    public Response listSnapshotShippings(
        @Context Request request,
        @QueryParam("nodes") List<String> nodes,
        @QueryParam("resources") List<String> resources,
        @QueryParam("snapshots") List<String> snapshots,
        @QueryParam("status") List<String> status,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_SNAPSHOT_SHIPPINGS, request, () ->
        {
            List<String> nodesFilter = nodes != null ? nodes : Collections.emptyList();
            List<String> resourcesFilter = resources != null ?
                resources.parallelStream().map(String::toLowerCase).collect(Collectors.toList()) :
                Collections.emptyList();
            List<String> statusFilter = status != null ? status : Collections.emptyList();

            Response response;

            Stream<SnapshotShippingListItemApi> snapsStream = ctrlApiCallHandler
                .listSnapshotShippings(nodesFilter, resourcesFilter, snapshots, statusFilter).stream();

            if (limit > 0)
            {
                snapsStream = snapsStream.skip(offset).limit(limit);
            }

            List<JsonGenTypes.SnapshotShippingStatus> snapshot = snapsStream
                .map(Json::apiToSnapshotShipping)
                .collect(Collectors.toList());

            response = RequestHelper.queryRequestResponse(
                objectMapper,
                ApiConsts.FAIL_NOT_FOUND_SNAPSHOT,
                "Snapshot shippments",
                null,
                snapshot
            );

            return response;
        }, false);
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
